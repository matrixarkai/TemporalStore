#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A diverged cluster inside a LIVE module that no production caller can reach.

`matrixark_mcp_context_pack` is live: four production modules import from it. Of its 44 top-level
definitions, TEN are reached by none of them, and SEVEN of those ten differ from every copy of the
same name elsewhere in the tree. Five of the seven are one cluster -- the ContextPack audit
compactors -- which call only each other.

This sits in the blind spot between the three checks that exist for this shape:

  * `test_a_module_only_tests_reach_is_not_live` and
    `test_an_unreachable_module_does_not_hold_a_diverged_copy` ask about MODULES. This module is
    reachable, so both pass.
  * `test_a_definition_nothing_reaches` asks about DEFINITIONS, and treats a name as an entry
    point when "any OTHER tracked file contains it as an identifier". It already records ONE
    orphan in this module -- the private `_default...` helper, whose identifier appears nowhere
    else. It cannot see the others, because every one of them is also DEFINED in
    `matrixark_mcp_core`, and that file's own `def` line seeds the name as reached. The rule is
    right for its purpose (`getattr` and `import_module` are real edges); the consequence is that
    it catches the orphan with a UNIQUE name and misses the orphan that is a DUPLICATE -- which
    is the more dangerous kind, because reading it gives a wrong answer rather than none.

So this file asks a narrower question the other three cannot: starting ONLY from the names other
PRODUCTION modules import from this module, and closing over the module's own top level, what does
it still not reach?

    matrixark_codex_hook              serving_async_pipeline_readiness
    matrixark_mcp_core                strip_default_debug_lineage_fields
    matrixark_mcp_core_context_pack   _context_memory_source_ref_is_debug_only,
                                      compact_context_pack_for_serving,
                                      selected_context_class_counts, serving_retrieval_metrics
    matrixark_mcp_core_packing        _pack_redundancy_filter_enabled, drop_redundant_pack_items,
                                      selected_ref_count_from_pack, session_continuity_counts

WHY TWO COUNTS AND NOT A LIST OF NAMES. `test_an_unreachable_module_does_not_hold_a_diverged_copy`
records an integer for a reason it states plainly: "A guard that writes down names feeds the
guards that count mentions -- that has happened three times here". It would happen again here.
Writing the private orphan's name into this file would make it "named in another tracked file",
which is exactly the rule `test_a_definition_nothing_reaches` seeds from, and its own recorded
entry for that name would stop matching. So the numbers are recorded, the names are derived at
run time, and they are printed only when an assertion fails.

MEASURED BY EXECUTION for the three of the cluster a fixture can separate. These three ARE named
below, and safely: each is also defined in `matrixark_mcp_core`, so every scan already sees the
name in another file and this one adds nothing.

  compact_context_pack_audit_record
      given a recall_policy carrying `backend_retrieval_pushdown`, the live copy emits a
      `backend_retrieval_pushdown` block of seven fields and this module's copy emits none.

  compact_context_pack_policy
      each copy keeps fields the other drops. This module's keeps `budget_semantics`,
      `remote_budget_tokens`, `derived`, `independent_caps` and `global_remote_budget_enforced`;
      the live copy keeps `selected_tokens_by_policy` and `selected_ref_count_by_phase`. They also
      disagree about ZERO: a `budget_tokens` entry of 0 survives here and is dropped there.

  compact_recall_policy_for_audit
      this module's copy shapes `async_pipeline_readiness` through
      `serving_async_pipeline_readiness`, dropping debug-only fields; the live copy passes the
      dict through unchanged.

Two of the seven differ in the SOURCE and were not shown to differ on any input tried, so nothing
is claimed about their behaviour -- only that their bodies are not the live ones and that nothing
reaches them either.

WHY THIS IS RECORDED AND NOT DELETED. Deleting is the obvious move and is not this file's to make:
reading an orphan before deleting it is a lesson this tree has already paid for, and two of the
ten are defined NOWHERE else, so a blanket delete loses code rather than duplicates. What is
asserted is the shape: the count may not grow, and a name that stops being orphaned makes it fall,
which fails here too and asks for the record to be updated.
"""
from __future__ import annotations

import ast
import hashlib
import importlib
import json
import os
import unittest
from typing import Dict, List, Set, Tuple

TOOLS = os.path.dirname(os.path.abspath(__file__))
MODULE = "matrixark_mcp_context_pack"
LIVE_TWIN = "matrixark_mcp_core"

#: Definitions no production importer reaches. An INTEGER, for the reason in the docstring.
#: Asserted exactly: it may not grow, and it may not fall without this record being updated.
RECORDED_UNREACHED = 10

#: Of those, the ones whose body matches no copy of the same name anywhere else.
RECORDED_DIVERGED = 7

#: A name that IS reached, so an empty answer cannot pass as a full one.
REACHED_CONTROL = "serving_async_pipeline_readiness"

DEFINITION_FLOOR = 30
IMPORTER_FLOOR = 3


def _import(stem):
    try:
        return importlib.import_module("tools." + stem)
    except ImportError:  # Direct script execution from tools/.
        return importlib.import_module(stem)


#: Parsing the tree is the whole cost of this file; each module is parsed once per process.
_PARSED: Dict[str, tuple] = {}
_DEFS: Dict[str, Dict[str, ast.AST]] = {}


def _parse(stem: str):
    if stem not in _PARSED:
        with open(os.path.join(TOOLS, stem + ".py"), encoding="utf-8", errors="replace") as handle:
            source = handle.read()
        _PARSED[stem] = (source, ast.parse(source))
    return _PARSED[stem]


def _definitions(stem: str) -> Dict[str, ast.AST]:
    if stem not in _DEFS:
        _source, tree = _parse(stem)
        _DEFS[stem] = {node.name: node for node in tree.body
                       if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))}
    return _DEFS[stem]


_ENTRY: Dict[bool, Tuple[Set[str], List[str]]] = {}
_REACHED: Dict[bool, Set[str]] = {}


def _entry_points(production_only: bool = True) -> Tuple[Set[str], List[str]]:
    """Names other modules import FROM this one, and which modules those are."""
    if production_only in _ENTRY:
        # A COPY. _reached() adds module-level names to what it gets back, and handing out the
        # cached set let that mutation leak into the next caller -- which made the production
        # set stop being a subset of the full one and failed the guard below for a reason that
        # was entirely this cache's fault.
        cached_names, cached_importers = _ENTRY[production_only]
        return set(cached_names), list(cached_importers)
    names: Set[str] = set()
    importers: List[str] = []
    for filename in sorted(os.listdir(TOOLS)):
        stem = filename[:-3]
        if not filename.endswith(".py") or stem == MODULE or filename.startswith("__"):
            continue
        if production_only and filename.startswith("test_"):
            continue
        try:
            _source, tree = _parse(stem)
        except (OSError, SyntaxError):  # pragma: no cover
            continue
        taken: Set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module and node.module.endswith(MODULE):
                taken |= {alias.name for alias in node.names}
            if isinstance(node, ast.Import) and any(
                    alias.name.endswith(MODULE) for alias in node.names):
                # A module-object import lets the importer touch anything on it.
                taken |= set(_definitions(MODULE))
        if taken:
            importers.append(stem)
            names |= taken
    _ENTRY[production_only] = (set(names), list(importers))
    return names, importers


def _reached(production_only: bool = True) -> Set[str]:
    if production_only in _REACHED:
        return set(_REACHED[production_only])
    defs = _definitions(MODULE)
    source, tree = _parse(MODULE)
    entry, _importers = _entry_points(production_only)
    for node in tree.body:  # module-level code runs on import and names things
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            continue
        for inner in ast.walk(node):
            if isinstance(inner, ast.Name):
                entry.add(inner.id)
            elif isinstance(inner, ast.Attribute):
                entry.add(inner.attr)
    if "__main__" in source:
        entry.add("main")
    reached: Set[str] = set()
    frontier = [name for name in entry if name in defs]
    while frontier:
        name = frontier.pop()
        if name in reached:
            continue
        reached.add(name)
        for inner in ast.walk(defs[name]):
            if isinstance(inner, ast.Name) and inner.id in defs:
                frontier.append(inner.id)
            elif isinstance(inner, ast.Attribute) and inner.attr in defs:
                frontier.append(inner.attr)
            elif isinstance(inner, ast.Constant) and isinstance(inner.value, str) \
                    and inner.value in defs:
                frontier.append(inner.value)
            elif isinstance(inner, ast.alias) and inner.name in defs:
                frontier.append(inner.name)
    _REACHED[production_only] = set(reached)
    return reached


def _unreached() -> Set[str]:
    return set(_definitions(MODULE)) - _reached()


def _body_digest(node: ast.AST) -> str:
    body = list(getattr(node, "body", []))
    if body and isinstance(body[0], ast.Expr) and isinstance(body[0].value, ast.Constant) \
            and isinstance(body[0].value.value, str):
        body = body[1:]
    return hashlib.sha256("\n".join(ast.dump(item) for item in body).encode()).hexdigest()[:12]


def _digests_elsewhere(names: Set[str]) -> Dict[str, Set[str]]:
    """name -> every body digest that name has ANYWHERE else in the production tree.

    One pass over the tree, not one per name: the per-name version parsed 290 modules ten times
    over and took 76 seconds, which is not a price a guard should charge every run.
    """
    out: Dict[str, Set[str]] = {name: set() for name in names}
    if not names:
        return out
    for filename in sorted(os.listdir(TOOLS)):
        stem = filename[:-3]
        if not filename.endswith(".py") or filename.startswith("test_") \
                or filename.startswith("__") or stem == MODULE:
            continue
        try:
            others = _definitions(stem)
        except (OSError, SyntaxError):  # pragma: no cover
            continue
        for name in names & set(others):
            out[name].add(_body_digest(others[name]))
    return out


def _diverged(names: Set[str]) -> Set[str]:
    """Of `names`, those whose body matches no copy anywhere else in the production tree."""
    mine = _definitions(MODULE)
    elsewhere = _digests_elsewhere(names)
    return {name for name in names
            if elsewhere[name] and _body_digest(mine[name]) not in elsewhere[name]}


PUSHDOWN = {
    "execution_mode": "native", "loaded_records": 12, "scanned_records": 340,
    "dropped_by_type": {"context_event": 4}, "index_postings_read": 9,
    "placement_partitions_touched": 2, "source": "engine",
}
RECALL_POLICY = {
    "backend_retrieval_pushdown": dict(PUSHDOWN),
    "async_pipeline_readiness": {"ready": True, "pending_tasks": 3, "debug_detail": "x"},
}
AUDIT_RECORD = {
    "record_type": "context_pack_audit", "context_pack_id": "cp-1", "query": "what happened",
    "summary_text": "s", "recall_policy": dict(RECALL_POLICY),
    "selected_refs": [{"ref_hash": 1, "text": "t"}], "created_at_ms": 1700000000000,
}
POLICY = {
    "mode": "auto", "budget_semantics": "independent", "remote_budget_tokens": 900,
    "derived": True, "independent_caps": True, "global_remote_budget_enforced": False,
    "selected_tokens_by_policy": {"a": 5}, "selected_ref_count_by_phase": {"p": 0},
    "budget_tokens": {"assistant": 0, "user": 30},
}


def _copy(stem, name):
    return getattr(_import(stem), name)


class ADivergedClusterNothingReaches(unittest.TestCase):

    def test_the_module_is_there_to_scan(self) -> None:
        """A floor. Every assertion below passes over an empty read."""
        defs = _definitions(MODULE)
        self.assertGreaterEqual(
            len(defs), DEFINITION_FLOOR,
            "%s has only %d top-level definitions; the scan is reading almost nothing"
            % (MODULE, len(defs)))
        _names, importers = _entry_points()
        self.assertGreaterEqual(
            len(importers), IMPORTER_FLOOR,
            "only %d production modules import from %s (%s). With none, EVERY definition reads as "
            "unreached and the counts below are meaningless."
            % (len(importers), MODULE, ", ".join(importers)))
        reached = _reached()
        self.assertIn(
            REACHED_CONTROL, reached,
            "%s is imported by matrixark_codex_hook and the scan calls it unreached, so the scan "
            "is broken, not the tree" % REACHED_CONTROL)
        self.assertGreater(
            len(reached), RECORDED_UNREACHED,
            "the scan reached fewer definitions than it called orphans, which is not a tree this "
            "file understands")

    def test_the_scan_ignores_test_modules(self) -> None:
        """This file must not make anything reachable by mentioning it."""
        production, _importers = _entry_points(production_only=True)
        everything, _all_importers = _entry_points(production_only=False)
        self.assertTrue(
            production <= everything,
            "the production entry-point set is not a subset of the full one, so one of the two "
            "scans is wrong")
        self.assertEqual(
            set(), production & _unreached(),
            "a definition this file counts as an orphan is now imported from %s by a PRODUCTION "
            "module: %s" % (MODULE, ", ".join(sorted(production & _unreached()))))

    def test_the_count_of_definitions_nothing_reaches_is_exactly_this(self) -> None:
        """Asserted in BOTH directions. Names are derived, and printed only on failure."""
        unreached = _unreached()
        self.assertEqual(
            RECORDED_UNREACHED, len(unreached),
            "%s now has %d definitions no production importer reaches, not %d. They are: %s"
            % (MODULE, len(unreached), RECORDED_UNREACHED, ", ".join(sorted(unreached))))

    def test_the_count_that_is_also_diverged_is_exactly_this(self) -> None:
        """The hazard, separated from the dead weight."""
        diverged = _diverged(_unreached())
        self.assertEqual(
            RECORDED_DIVERGED, len(diverged),
            "%d of %s's orphans now differ from every copy elsewhere, not %d. An orphan that "
            "matches its live twin is dead weight; one that does not is a wrong answer waiting to "
            "be read. They are: %s"
            % (len(diverged), MODULE, RECORDED_DIVERGED, ", ".join(sorted(diverged))))
        self.assertLess(
            RECORDED_DIVERGED, RECORDED_UNREACHED,
            "every orphan is recorded as diverged, which would mean this file is not "
            "distinguishing the two kinds at all")

    def test_the_audit_record_drops_a_block_the_live_copy_emits(self) -> None:
        """Executed. The clearest of the three measurable differences."""
        live = _copy(LIVE_TWIN, "compact_context_pack_audit_record")(
            json.loads(json.dumps(AUDIT_RECORD)), include_debug=False)
        orphan = _copy(MODULE, "compact_context_pack_audit_record")(
            json.loads(json.dumps(AUDIT_RECORD)), include_debug=False)
        self.assertEqual(
            sorted(PUSHDOWN), sorted(live.get("backend_retrieval_pushdown", {})),
            "the live copy no longer emits the whole backend_retrieval_pushdown block, so this "
            "comparison is not measuring what it says")
        self.assertNotIn(
            "backend_retrieval_pushdown", orphan,
            "%s's orphan copy now emits backend_retrieval_pushdown too -- the bodies converged, "
            "so say which won" % MODULE)

    def test_the_policy_compactors_each_keep_fields_the_other_drops(self) -> None:
        """Executed, in BOTH directions, plus the disagreement about zero."""
        live = _copy(LIVE_TWIN, "compact_context_pack_policy")(json.loads(json.dumps(POLICY)))
        orphan = _copy(MODULE, "compact_context_pack_policy")(json.loads(json.dumps(POLICY)))
        self.assertEqual(
            ["selected_ref_count_by_phase", "selected_tokens_by_policy"],
            sorted(key for key in live if key not in orphan),
            "the live policy compactor now keeps a different set of fields the orphan drops")
        self.assertEqual(
            ["budget_semantics", "derived", "global_remote_budget_enforced", "independent_caps",
             "remote_budget_tokens"],
            sorted(key for key in orphan if key not in live),
            "the orphan policy compactor now keeps a different set of fields the live copy drops")
        self.assertEqual(
            {"user": 30}, live.get("budget_tokens"),
            "the live copy no longer drops a zero budget_tokens entry")
        self.assertEqual(
            {"assistant": 0, "user": 30}, orphan.get("budget_tokens"),
            "the orphan no longer keeps a zero budget_tokens entry")

    def test_only_one_recall_compactor_shapes_the_readiness_block(self) -> None:
        """Executed. The orphan is the one that strips, which is the surprising direction."""
        live = _copy(LIVE_TWIN, "compact_recall_policy_for_audit")(
            json.loads(json.dumps(RECALL_POLICY)))
        orphan = _copy(MODULE, "compact_recall_policy_for_audit")(
            json.loads(json.dumps(RECALL_POLICY)))
        self.assertEqual(
            RECALL_POLICY["async_pipeline_readiness"], live.get("async_pipeline_readiness"),
            "the live copy no longer passes async_pipeline_readiness through unchanged")
        self.assertEqual(
            {"pending_tasks", "ready"}, set(orphan.get("async_pipeline_readiness", {})),
            "the orphan now keeps a different set of readiness fields, so the two have converged "
            "or drifted further")


if __name__ == "__main__":
    unittest.main()
