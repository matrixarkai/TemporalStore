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
import unittest
from typing import Dict, List, Tuple

TOOLS = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(TOOLS)
INVENTORY = os.path.join(REPO, "docs", "ops", "temporalstore-engine-flags.md")

# Every boolean whose other arm no shipped selector reaches, and why it keeps one.
KNOWN_TWO_PATH_FLAGS: Dict[str, str] = {
    "MATRIXARK_CONTEXT_SECONDARY_INDEX":
        "retrieval does not read the ctxidx refs, so ON is write-only cost on the live path -- but "
        "ON is what the harness query-back validation reads, and the accessor calls it the escape "
        "hatch for anything still on that surface, held until a redesign gives the refs a reader",
    "MATRIXARK_CONTEXT_PACK_DEBUG_LINEAGE":
        "OFF strips ref_hash, node_hash, node_path and continuity_reason from a served ref; ON "
        "returns them. ON reads what OFF suppresses, which is the whole point of a diagnostic",
    "MATRIXARK_CONTEXT_PACK_INCLUDE_SCORES":
        "the same, for the scores behind a pack's ordering",
    "MATRIXARK_CONTEXT_RETRIEVE_TRACE":
        "ON emits per-stage timings from retrieve_context; OFF emits none. The only way to see "
        "where a slow retrieval spent its time",
    "TEMPORALSTORE_CONTEXT_BENCHMARK_ALL_SOURCE_REPLAY": "benchmark harness mode",
    "TEMPORALSTORE_CONTEXT_BENCHMARK_COMPACT_SOURCE_REPLAY": "benchmark harness mode",
    "TEMPORALSTORE_CONTEXT_BENCHMARK_DIRECT_SOURCE_SCORING": "benchmark harness mode",
    "TEMPORALSTORE_CONTEXT_BENCHMARK_EXTERNAL_ONLY": "benchmark harness mode",
    "TEMPORALSTORE_CONTEXT_BENCHMARK_REPORT_ONLY": "benchmark harness mode",
    "TEMPORALSTORE_CONTEXT_BENCHMARK_SOURCE_ORDER_RANKING": "benchmark harness mode",
    "TEMPORALSTORE_CONTEXT_BENCHMARK_STORED_RECORD_SCORING": "benchmark harness mode",
    "TEMPORALSTORE_CONTEXT_BENCHMARK_TRACE": "benchmark harness mode",
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


def _unselected() -> List[str]:
    """Booleans whose other arm nothing ships a selector for."""
    out = []
    for name, _default, set_by in _rows():
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
