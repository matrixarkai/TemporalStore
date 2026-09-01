#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""A tenant-policy gate that nothing calls is a knob that resolves correctly and changes nothing.

This is the worst shape a configuration surface can take. The knob is in the registry, the portal
offers it, `resolve()` returns exactly what the tenant set, the docstring explains what it does --
and the write path never asks. Nothing errors, nothing logs, and the only way to notice is to ingest
under two opposite policies and compare what was stored.

Six of the fourteen gates are in that state today. They are listed rather than fixed-by-assertion so
this file can land ahead of the wiring and hold the line meanwhile: a NEW unwired gate fails, and a
listed gate that gets wired ALSO fails, telling you to strike it off. A list that is allowed to
silently stay wrong is the same failure one level up.
"""
from __future__ import annotations

import os
import re
import subprocess
import sys
import unittest
from typing import Dict, List, Set

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

TOOLS = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(TOOLS)
GATES_MODULE = os.path.join(TOOLS, "matrixark_index_growth_bound.py")

# Known unwired as of 2026-09-01, verified three ways: no call to the gate, no direct
# resolve_tenant_policy() for the knob, and no read of the knob's env var in any .py or .rs.
# Shrink this list as they are wired -- the test fails if you forget to.
KNOWN_UNWIRED: Set[str] = {
    "extract_segments_enabled",
    "generate_embeddings_enabled",
    "node_path_embeddings_enabled",
    "return_all_candidates_enabled",
    "store_event_summary_text_enabled",
    "traverse_sibling_sessions_enabled",
}

# The census found 14. If a refactor moves gates elsewhere this number drops and every "no unwired
# gate found" assertion below becomes vacuously true, so the count is asserted too -- a guard that
# scans source has to assert its own extent or it silently degrades into checking nothing.
EXPECTED_GATE_FLOOR = 14


def _gates() -> List[str]:
    with open(GATES_MODULE, encoding="utf-8") as handle:
        return re.findall(r"^def ([a-z0-9_]+_enabled)\(", handle.read(), re.M)


def _production_sources() -> List[str]:
    listed = subprocess.run(["git", "ls-files", "*.py"], cwd=REPO,
                            capture_output=True, text=True).stdout.split()
    return [path for path in listed
            if not os.path.basename(path).startswith("test_")]


def _callers() -> Dict[str, List[str]]:
    """Production call sites per gate. A definition, an import and a comment are not calls."""
    gates = _gates()
    found: Dict[str, List[str]] = {gate: [] for gate in gates}
    for path in _production_sources():
        try:
            with open(os.path.join(REPO, path), encoding="utf-8") as handle:
                lines = handle.read().splitlines()
        except OSError:
            continue
        for number, line in enumerate(lines, 1):
            stripped = line.strip()
            if stripped.startswith(("import ", "from ", "#")):
                continue
            for gate in gates:
                if gate not in line or stripped.startswith("def %s(" % gate):
                    continue
                found[gate].append("%s:%d" % (path, number))
    return found


class PolicyGatesAreWiredTest(unittest.TestCase):

    def test_the_census_still_finds_the_gates(self) -> None:
        # Asserted so the checks below cannot pass by finding nothing to check.
        gates = _gates()
        self.assertGreaterEqual(
            len(gates), EXPECTED_GATE_FLOOR,
            "found %d policy gates, expected at least %d -- if they moved, this file is no longer "
            "looking where they live and every assertion under it is vacuous"
            % (len(gates), EXPECTED_GATE_FLOOR))

    def test_no_new_gate_is_left_unwired(self) -> None:
        callers = _callers()
        unwired = {gate for gate, sites in callers.items() if not sites}
        new = sorted(unwired - KNOWN_UNWIRED)
        self.assertEqual(
            [], new,
            "these tenant-policy gates have no production caller, so the knobs they guard resolve "
            "correctly and change nothing: %s. A customer sets them, the portal shows them, and "
            "the write path never asks." % new)

    def test_a_gate_that_got_wired_is_struck_off_the_list(self) -> None:
        callers = _callers()
        wired = sorted(gate for gate in KNOWN_UNWIRED if callers.get(gate))
        self.assertEqual(
            [], wired,
            "these are listed as known-unwired but now have callers: %s. Remove them from "
            "KNOWN_UNWIRED -- a list of known problems that is allowed to go stale hides the fix "
            "as effectively as it hid the defect." % wired)

    def test_every_listed_gate_still_exists(self) -> None:
        gates = set(_gates())
        vanished = sorted(KNOWN_UNWIRED - gates)
        self.assertEqual(
            [], vanished,
            "KNOWN_UNWIRED names gates that no longer exist: %s. A renamed gate leaves an entry "
            "here that excuses nothing and silently shrinks what is checked." % vanished)


if __name__ == "__main__":
    unittest.main()
