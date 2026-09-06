#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A boolean flag nothing ships a selector for must say why it still has two paths.

One live path per flag is the aim: keep the logic, keep the path the deployment takes, and stop
carrying an arm nobody can reach. Most engine booleans are not in question -- a launcher, a config
file or the portal selects them, so both arms are reachable on purpose.

The ones worth a decision are the booleans whose other arm NOTHING in the repository selects, or
which only a test selects. For those the deployment has one path already, and the second arm is
either a hatch worth keeping or code worth removing. Each one below was looked at and the reason
written down; the set is asserted exactly, so a NEW one fails here until somebody decides about it,
and one that stops qualifying fails too rather than sitting in a list that has quietly become
fiction.

What the reasons have in common is the test that settled them: does the other arm let this build
DO or READ something the live arm cannot? A diagnostic that surfaces suppressed fields does. A
benchmark mode that the serving path never reaches does, for the harness. A branch that produces
the same answer more slowly does not, and would be the one to remove.

Read from the generated inventory rather than by re-parsing Rust: that document is built from the
source and a sibling test asserts regenerating it changes nothing, so it is the source one step
removed rather than a second opinion about it.
"""
from __future__ import annotations

import os
import re
import subprocess
import unittest
from typing import Dict, List, Tuple

TOOLS = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(TOOLS)
INVENTORY = os.path.join(REPO, "docs", "ops", "temporalstore-engine-flags.md")

# Every boolean whose other arm no shipped selector reaches, and why it keeps one.
KNOWN_TWO_PATH_FLAGS: Dict[str, str] = {
    # The arm that produces a benchmark result the project refuses to publish. Turning it on
    # scores the source text directly instead of retrieving it, and marks the run
    # rust_context_event_ingest=false; validate_benchmark_claims.py and
    # archive_context_benchmark_report.py both reject a report whose
    # rust_temporalstore_direct_source_scoring is true. The harness pins it to "0", its own
    # default, so the published field is always false. The branch is what gives those refusals
    # something to refuse, and it is the only way to measure the ceiling a real retrieval is
    # scored against, so it stays.
    "TEMPORALSTORE_CONTEXT_BENCHMARK_DIRECT_SOURCE_SCORING": "benchmark ceiling, refused when published",
}

# The eight benchmark modes are one decision, not eight, and it is worth saying so once: they live
# in a single binary that no serving path links, so the deployment has one path whatever they say.
_BENCHMARK_BINARY = "src/bin/context_workflow_harness.rs"

# 34 booleans had a readable default when this was written.
EXPECTED_BOOLEAN_FLOOR = 20


def _rows() -> List[Tuple[str, str, str]]:
    """(flag, default, set-by) for every inventory row whose default is a boolean.

    Booleans only. A flag whose default the inventory could not read is not a two-path question
    this file can answer -- there is no telling which arm is the live one -- and a flag carrying a
    number has one path with a dial on it. MATRIXARK_BULK_INGEST_EXPECTED_WAL_COMMANDS is the
    nearest miss: only a test sets it, but its default is unreadable, so it is out of scope here
    rather than silently counted as decided.
    """
    rows = []
    with open(INVENTORY, encoding="utf-8") as handle:
        for line in handle:
            if not line.startswith("|"):
                continue
            cells = [cell.strip() for cell in line.strip().strip("|").split("|")]
            if len(cells) < 5:
                continue
            name = cells[0].strip("`")
            if not re.fullmatch(r"[A-Z][A-Z0-9_]+", name):
                continue
            if cells[1] not in ("on", "off"):
                continue
            rows.append((name, cells[1], cells[2]))
    return rows


def _assigned_values() -> Dict[str, set]:
    """Every flag name a tracked file ASSIGNS, anywhere in the repository.

    The inventory's own `set by` column cannot answer this: it is built from engine sources, so a
    flag a Python harness or a shell script sets reads there as selected by nothing. Most of the
    benchmark flags were listed that way while `tools/run_locomo_rust_harness.py` sets them,
    several to their non-default arm. A list that says "nothing selects this" about a flag
    something selects is worse than no list, because it licenses a removal.

    An assignment is only a selector when it writes something OTHER than the flag's own default:
    pinning a flag to the value it already has reaches no second arm. So each name is mapped to
    the literal values assigned to it, and a name assigned by a form not readable as a literal --
    a conditional, an f-string, a variable -- keeps counting as a selector. That is the safe
    direction to be wrong in: it leaves a flag on the list to look at again, where the other
    direction would license removing a branch something reaches.
    """
    listed = subprocess.run(["git", "ls-files"], cwd=REPO,
                            capture_output=True, text=True).stdout.split()
    assign = re.compile(
        r'"((?:TS|MATRIXARK|TEMPORALSTORE)_[A-Z0-9_]+)"\s*:'                 # a dict entry
        r'|export\s+((?:TS|MATRIXARK|TEMPORALSTORE)_[A-Z0-9_]+)='            # a shell export
        r'|environ\[\s*"((?:TS|MATRIXARK|TEMPORALSTORE)_[A-Z0-9_]+)"\s*\]\s*='  # a python set
        r'|set_var\(\s*"((?:TS|MATRIXARK|TEMPORALSTORE)_[A-Z0-9_]+)"')      # a rust set
    literal = re.compile(
        r""""((?:TS|MATRIXARK|TEMPORALSTORE)_[A-Z0-9_]+)"\s*:\s*"([^"]*)"\s*,?\s*$"""
        r"""|export\s+((?:TS|MATRIXARK|TEMPORALSTORE)_[A-Z0-9_]+)=["']?([A-Za-z01]*)["']?\s*$""")
    found: Dict[str, set] = {}
    for path in listed:
        if os.path.splitext(path)[1] not in (".py", ".sh", ".rs", ".toml", ".json", ".yaml"):
            continue
        base = os.path.basename(path)
        # This file lists the flag names it has decided about, in a dict. Reading itself would
        # make every decision look like a selector and empty the list -- the same self-reference
        # that made an earlier guard feed on its own output.
        if base == os.path.basename(__file__):
            continue
        # A test exercising the other arm is not a SHIPPED selector; that is the distinction the
        # list is about, and the inventory column already reports it separately.
        if (base.startswith("test_") or base in ("tests.rs", "tests.py")
                or "/tests/" in path or "/tests." in path):
            continue
        try:
            with open(os.path.join(REPO, path), encoding="utf-8", errors="replace") as handle:
                source = handle.read()
        except OSError:
            continue
        for match in assign.finditer(source):
            found.setdefault(next(group for group in match.groups() if group), set()).add(None)
        for line in source.splitlines():
            hit = literal.search(line)
            if hit:
                name = hit.group(1) or hit.group(3)
                value = hit.group(2) if hit.group(1) else hit.group(4)
                found.setdefault(name, set()).discard(None)
                found[name].add(value)
    return found


_ON_LITERALS = {"1", "true", "TRUE", "yes", "YES", "True"}
_OFF_LITERALS = {"0", "false", "FALSE", "no", "NO", "False", ""}


def _reaches_the_other_arm(values: set, default: str) -> bool:
    """True when some assignment writes a value the default does not already have."""
    if None in values or not values:
        return True
    already = _ON_LITERALS if default == "on" else _OFF_LITERALS
    return any(value not in already for value in values)


def _unselected() -> List[str]:
    """Booleans whose other arm nothing ships a selector for, asked of the whole tree."""
    assigned = _assigned_values()
    out = []
    for name, _default, set_by in _rows():
        if name in assigned and _reaches_the_other_arm(assigned[name], _default):
            continue
        if set_by == "nothing" or set(part.strip() for part in set_by.split(",")) <= {
                "test", "harness"}:
            out.append(name)
    return sorted(out)


class ATwoPathFlagSaysWhyTest(unittest.TestCase):

    def test_the_inventory_still_reports_boolean_defaults(self) -> None:
        rows = _rows()
        self.assertGreaterEqual(
            len(rows), EXPECTED_BOOLEAN_FLOOR,
            "found %d booleans with a readable default, expected at least %d -- if the document's "
            "shape changed, every assertion below runs on an empty set"
            % (len(rows), EXPECTED_BOOLEAN_FLOOR))

    def test_every_unselected_boolean_has_a_decision(self) -> None:
        undecided = sorted(set(_unselected()) - set(KNOWN_TWO_PATH_FLAGS))
        self.assertEqual(
            [], undecided,
            "these booleans carry a second path that nothing in the repository selects, and "
            "nobody has said why: %s\nDecide: if the other arm lets the build do or read "
            "something the live arm cannot, add it above with that reason. If it does not, remove "
            "the branch and keep the live path." % undecided)

    def test_a_decision_that_no_longer_applies_is_struck_off(self) -> None:
        stale = sorted(set(KNOWN_TWO_PATH_FLAGS) - set(_unselected()))
        self.assertEqual(
            [], stale,
            "these are listed as having no shipped selector and now have one, or have gone "
            "entirely: %s. Strike them off -- a list of decisions that is allowed to go stale "
            "describes a tree that no longer exists." % stale)

    def test_every_decision_gives_a_reason(self) -> None:
        empty = sorted(name for name, why in KNOWN_TWO_PATH_FLAGS.items() if len(why.strip()) < 12)
        self.assertEqual(
            [], empty,
            "these are listed without a reason worth reading: %s. The list is only useful if the "
            "next person can tell a hatch from an oversight without repeating the work." % empty)

    def test_the_benchmark_modes_really_are_confined_to_the_harness(self) -> None:
        """The one claim above that is made eight times, checked once."""
        path = os.path.join(REPO, "crates", "temporalstore-rust", _BENCHMARK_BINARY)
        self.assertTrue(os.path.exists(path), "the benchmark harness moved: %s" % path)
        with open(path, encoding="utf-8") as handle:
            harness = handle.read()
        for name, why in sorted(KNOWN_TWO_PATH_FLAGS.items()):
            if why != "benchmark harness mode":
                continue
            with self.subTest(flag=name):
                # assertTrue, not assertIn: the haystack is the whole harness, and assertIn
                # prints it. A failure here should name the flag, not paste a binary at you.
                self.assertTrue(
                    name in harness,
                    "%s is filed as a benchmark mode but the harness does not mention it, so the "
                    "reason is about some other flag" % name)


if __name__ == "__main__":
    unittest.main()
