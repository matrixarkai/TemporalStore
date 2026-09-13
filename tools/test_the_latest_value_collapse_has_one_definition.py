#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Two copies of the latest-value collapse, and they break a tie differently.

`compact_latest_value_records` keeps one record per latest-value identity. It is defined twice,
and both copies run:

    matrixark_mcp_local_adapter    the serving read path -- `apply_memory_tombstones(
                                   compact_latest_value_records(sweep_index_compaction(records)))`
    matrixark_mcp_latest_values    reached by matrixark_mcp_recovery, which pyproject.toml
                                   publishes as the console script `matrixark-local-recovery`

They agree about WHICH records share an identity. They disagree about which of two records with
the SAME `updated_at_ms` wins:

    matrixark_mcp_local_adapter    compares (timestamp, profile_revision or revision)
    matrixark_mcp_latest_values    compares the timestamp alone, with `>=`, so on a tie the
                                   LAST record seen in the log wins regardless of revision

MEASURED BY EXECUTION on two `context_entity` rows sharing an entity_hash and an `updated_at_ms`,
with `profile_revision` 1 and 5:

    input order [rev 5, rev 1]    serving keeps rev 5     recovery keeps rev 1
    input order [rev 1, rev 5]    serving keeps rev 5     recovery keeps rev 5

So the recovery report's view of the store is order-dependent where serving's is not, and on a tie
it can report the SUPERSEDED profile revision as the surviving one.

`profile_revision` is live data on exactly that record type: `matrixark_local_adapter_ingest` and
`matrixark_mcp_local_batch_extract_runtime` both stamp it on profile entity records, and
`matrixark_mcp_core_packing` reads it back when ranking. NOT CLAIMED: how often two revisions of
one entity land in the same millisecond. What is claimed is that when they do, the two copies
choose differently, and only one of them consults the field written to break exactly this tie.

WHAT IS NOT A FINDING, recorded so nobody re-derives it. The sibling `latest_value_record_key` is
ALSO defined in both modules, and the two look very different -- the serving copy prefers a
stamped `row_key` field and falls back to a per-type function, the recovery copy only has the
per-type logic. Compared across all 12 record types either one names, on a fully populated
record, they return the SAME key every time. The comment above the `resource_import_task` branch
in `matrixark_mcp_latest_values` says "matrixark_mcp_local_adapter fixed this in its own copy and
this copy was not carried along"; it has since been carried along, and that sentence now reads
more alarming than the code is.

THIS FILE DOES NOT ASSERT THAT THE COPIES AGREE. Teaching the recovery copy the revision tie-break
changes what an operator's recovery report says a store contains; taking it out of the serving
copy changes which profile revision is served. Either is a decision. Recorded in BOTH directions:
a new divergence fails here and a resolved one fails here too.
"""
from __future__ import annotations

import ast
import copy
import importlib
import os
import unittest
from typing import Dict, Set, Tuple

TOOLS = os.path.dirname(os.path.abspath(__file__))

FUNCTION = "compact_latest_value_records"
KEY_FUNCTION = "latest_value_record_key"

#: Every live copy, and what reaches it.
LIVE_COPIES: Dict[str, str] = {
    "matrixark_mcp_local_adapter":
        "the serving read path calls it inside apply_memory_tombstones(...)",
    "matrixark_mcp_latest_values":
        "matrixark_mcp_recovery imports it, and pyproject.toml publishes that module's main as "
        "the console script matrixark-local-recovery",
}

#: How the second copy is on a live path at all: an installed entry point, not an import.
CONSOLE_SCRIPT_OWNER = "matrixark_mcp_recovery"
CONSOLE_SCRIPT_LINE = "tools.matrixark_mcp_recovery:main"

ENTITY_HASH = 77
SHARED_TIMESTAMP_MS = 1_000

#: (input order) -> {module: the profile_revision that copy keeps}. Asserted exactly, both ways.
RECORDED_TIE_WINNER = {
    "highest revision first": {
        "records": (5, 1),
        "matrixark_mcp_local_adapter": 5,
        "matrixark_mcp_latest_values": 1,
    },
    "highest revision last": {
        "records": (1, 5),
        "matrixark_mcp_local_adapter": 5,
        "matrixark_mcp_latest_values": 5,
    },
}

#: Modules that stamp profile_revision onto a record, so the tie-break field is real data.
REVISION_WRITERS = ("matrixark_local_adapter_ingest", "matrixark_mcp_local_batch_extract_runtime")

#: A fully populated record of every type either key function names, to compare their answers.
KEY_PROBE_FIELDS = {
    "node_hash": 1, "child_ref_hash": 2, "event_id_hash": 3, "summary_type": "s",
    "summary_hash": 4, "embedding_type": "e", "ref_type": "node", "ref_hash": 5,
    "index_name": "i", "scope_key": "sk", "data_model": "dm", "timestamp_key_ms": 9,
    "entity_hash": 6, "dirty_hash": 7, "buffer_key": ["b"], "resource_hash": 8,
    "skill_hash": 9, "task_hash": 10, "updated_at_ms": 11, "node_id": 12, "capability": "c",
}
RECORD_TYPE_FLOOR = 10
MODULE_SCAN_FLOOR = 200


def _import(stem):
    try:
        return importlib.import_module("tools." + stem)
    except ImportError:  # Direct script execution from tools/.
        return importlib.import_module(stem)


def _defining_modules(function: str) -> Tuple[Set[str], int]:
    found: Set[str] = set()
    scanned = 0
    for name in sorted(os.listdir(TOOLS)):
        if not name.endswith(".py") or name.startswith("test_") or name.startswith("__"):
            continue
        scanned += 1
        try:
            with open(os.path.join(TOOLS, name), encoding="utf-8", errors="replace") as handle:
                tree = ast.parse(handle.read())
        except (OSError, SyntaxError):  # pragma: no cover
            continue
        for node in tree.body:
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == function:
                found.add(name[:-3])
    return found, scanned


def _entity(revision: int) -> Dict[str, object]:
    return {"record_type": "context_entity", "entity_hash": ENTITY_HASH,
            "updated_at_ms": SHARED_TIMESTAMP_MS, "profile_revision": revision,
            "state": "rev-%d" % revision}


def _record_types_named(stem: str, function: str) -> Set[str]:
    with open(os.path.join(TOOLS, stem + ".py"), encoding="utf-8", errors="replace") as handle:
        tree = ast.parse(handle.read())
    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) or node.name != function:
            continue
        out: Set[str] = set()
        for inner in ast.walk(node):
            if isinstance(inner, ast.Compare) and isinstance(inner.left, ast.Name) \
                    and inner.left.id == "record_type":
                for comparator in inner.comparators:
                    if isinstance(comparator, ast.Constant) and isinstance(comparator.value, str):
                        out.add(comparator.value)
        return out
    return set()


class TwoLatestValueCollapses(unittest.TestCase):

    def test_the_copies_are_there_to_compare(self) -> None:
        """A floor and the anchor."""
        found, scanned = _defining_modules(FUNCTION)
        self.assertGreaterEqual(
            scanned, MODULE_SCAN_FLOOR,
            "the definition scan read only %d production modules" % scanned)
        self.assertEqual(
            sorted(LIVE_COPIES), sorted(found),
            "%s is defined by a different set of production modules than this file records"
            % FUNCTION)
        objects = {stem: getattr(_import(stem), FUNCTION) for stem in LIVE_COPIES}
        for stem, bound in objects.items():
            with self.subTest(module=stem):
                self.assertEqual(
                    stem, bound.__module__.rsplit(".", 1)[-1],
                    "%s.%s is owned by %s" % (stem, FUNCTION, bound.__module__))
        self.assertEqual(
            len(LIVE_COPIES), len({id(bound) for bound in objects.values()}),
            "the copies are the same object; the split is resolved")

    def test_the_recovery_copy_is_on_a_live_path(self) -> None:
        """It is reached by an INSTALLED console script, not by an import from the server."""
        with open(os.path.join(os.path.dirname(TOOLS), "pyproject.toml"),
                  encoding="utf-8", errors="replace") as handle:
            pyproject = handle.read()
        self.assertIn(
            CONSOLE_SCRIPT_LINE, pyproject,
            "pyproject.toml no longer publishes %s. If nothing runs that module, the second copy "
            "is not live and this record should say so instead." % CONSOLE_SCRIPT_LINE)
        with open(os.path.join(TOOLS, CONSOLE_SCRIPT_OWNER + ".py"),
                  encoding="utf-8", errors="replace") as handle:
            owner = handle.read()
        self.assertIn(
            "matrixark_mcp_latest_values", owner,
            "%s no longer imports matrixark_mcp_latest_values" % CONSOLE_SCRIPT_OWNER)
        self.assertEqual(
            "matrixark_mcp_latest_values",
            getattr(_import(CONSOLE_SCRIPT_OWNER), FUNCTION).__module__.rsplit(".", 1)[-1],
            "%s now binds a different copy of %s" % (CONSOLE_SCRIPT_OWNER, FUNCTION))

    def test_the_fixture_reaches_the_tie_break(self) -> None:
        """A floor on the FIXTURE, not the code.

        Both copies agree on every input where the timestamps differ, so a fixture whose
        timestamps drifted apart would agree for the most boring reason.
        """
        first, second = _entity(1), _entity(5)
        self.assertEqual(
            first["updated_at_ms"], second["updated_at_ms"],
            "the two probe records no longer share a timestamp, so the tie-break is never reached")
        self.assertNotEqual(
            first["profile_revision"], second["profile_revision"],
            "the two probe records no longer differ in revision")
        for stem in LIVE_COPIES:
            with self.subTest(module=stem):
                key = getattr(_import(stem), KEY_FUNCTION)(copy.deepcopy(first))
                self.assertIsNotNone(
                    key, "%s gives the probe record no latest-value key, so it is never "
                         "collapsed and nothing below is being compared" % stem)
                self.assertEqual(
                    key, getattr(_import(stem), KEY_FUNCTION)(copy.deepcopy(second)),
                    "%s no longer gives the two probe records the SAME key, so they do not "
                    "compete and the tie-break is not reached" % stem)
        # And the copies agree when the timestamps do NOT tie, which is what makes the tie the
        # finding rather than a general disagreement.
        older, newer = _entity(5), _entity(1)
        newer["updated_at_ms"] = SHARED_TIMESTAMP_MS + 1
        kept = {stem: getattr(_import(stem), FUNCTION)(
            [copy.deepcopy(older), copy.deepcopy(newer)]) for stem in LIVE_COPIES}
        for stem, records in kept.items():
            with self.subTest(module=stem, case="different timestamps"):
                self.assertEqual(
                    [newer["state"]], [record["state"] for record in records],
                    "%s no longer keeps the newer record when the timestamps differ" % stem)

    def test_the_tie_winner_each_copy_keeps_is_exactly_this(self) -> None:
        """Asserted per copy and per input order, in both directions."""
        measured: Dict[str, Dict[str, object]] = {}
        for label, case in RECORDED_TIE_WINNER.items():
            records = [_entity(revision) for revision in case["records"]]
            measured[label] = {}
            for stem in LIVE_COPIES:
                with self.subTest(case=label, module=stem):
                    kept = getattr(_import(stem), FUNCTION)(copy.deepcopy(records))
                    self.assertEqual(
                        1, len(kept),
                        "%s no longer collapses the two records to one" % stem)
                    measured[label][stem] = kept[0].get("profile_revision")
                    self.assertEqual(
                        case[stem], measured[label][stem],
                        "%s now keeps a different revision on a tie" % stem)
        # Asserted on what was MEASURED, not on the constants above: a record edited into
        # agreement would otherwise satisfy this while the code still disagreed.
        self.assertNotEqual(
            measured["highest revision first"]["matrixark_mcp_latest_values"],
            measured["highest revision first"]["matrixark_mcp_local_adapter"],
            "the two copies now keep the same revision on the case this file exists for, which "
            "means the split is resolved -- strike this file and say which won")

    def test_the_tie_break_field_is_written_by_a_live_path(self) -> None:
        """Why the tie-break matters: something stamps the field the serving copy reads."""
        for stem in REVISION_WRITERS:
            with self.subTest(module=stem):
                with open(os.path.join(TOOLS, stem + ".py"),
                          encoding="utf-8", errors="replace") as handle:
                    body = handle.read()
                self.assertIn(
                    '"profile_revision":', body,
                    "%s no longer writes profile_revision onto a record. If nothing does, the "
                    "tie-break reads a field that is never set and this record should say so."
                    % stem)

    def test_the_key_functions_are_not_the_finding(self) -> None:
        """Recorded so the sibling is not re-derived: they LOOK different and answer the same."""
        names = {"matrixark_mcp_latest_values": KEY_FUNCTION,
                 "matrixark_mcp_local_adapter": "_latest_value_record_key_by_type"}
        types = {stem: _record_types_named(stem, function) for stem, function in names.items()}
        for stem, found in types.items():
            with self.subTest(module=stem):
                self.assertGreaterEqual(
                    len(found), RECORD_TYPE_FLOOR,
                    "only %d record types were parsed out of %s.%s, so the comparison below "
                    "covers almost nothing" % (len(found), stem, names[stem]))
        self.assertEqual(
            sorted(types["matrixark_mcp_latest_values"]),
            sorted(types["matrixark_mcp_local_adapter"]),
            "the two per-type key functions no longer name the same record types, which WOULD be "
            "a finding -- one of them can no longer collapse a type the other can")
        every_type = set().union(*types.values())
        disagreeing = []
        for record_type in sorted(every_type):
            record = {"record_type": record_type}
            record.update(KEY_PROBE_FIELDS)
            answers = {stem: getattr(_import(stem), function)(dict(record))
                       for stem, function in names.items()}
            if len(set(map(repr, answers.values()))) > 1:
                disagreeing.append((record_type, answers))
        self.assertEqual(
            [], [record_type for record_type, _answers in disagreeing],
            "the per-type key functions now disagree for these record types, which is a SECOND "
            "divergence this file does not yet describe: %s"
            % ", ".join("%s %s" % (record_type, answers)
                        for record_type, answers in disagreeing))


if __name__ == "__main__":
    unittest.main()
