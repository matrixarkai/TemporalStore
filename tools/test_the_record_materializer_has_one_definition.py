#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""The record materialiser is defined twice, and this time the COMPACT copy is the richer one.

`test_the_record_shaping_helpers_have_one_definition` records the same module pair, for the two
helpers `materialize_serving_records` calls: `attach_storage_route` and `compact_record_scope`. It
asserts that both modules still define `materialize_serving_records` and then says nothing about
its body, and nothing at all about `materialize_serving_record_batch`. This file is the rest of
it. A guard covering one function of a diverged pair leaves the others free to drift, and they
did.

Which copy each live path reaches, read off `fn.__module__` rather than from import statements:

    matrixark_context_backfill          -> matrixark_mcp_core_compact
    matrixark_temporal_direct_backend   -> matrixark_mcp_core_compact   (via `import *` on core)
    matrixark_mcp_local_adapter         -> matrixark_mcp_serving_records
    matrixark_mcp_server                -> matrixark_mcp_serving_records
    matrixark_mcp_temporal_adapters     -> matrixark_mcp_serving_records (through the adapter)

MEASURED BY EXECUTION. Both copies were imported and run on the same records.

  1. A context_embedding carrying `source_event_ids` and `source_segment_hashes`:
     matrixark_mcp_core_compact writes `source_event_count` and `source_segment_count`, and
     matrixark_mcp_serving_records writes neither. This is the direction the sibling file does
     NOT record -- there, "core_compact is behind" holds in every case. Here it is ahead, so the
     split cannot be resolved by declaring one module the winner.

  2. `materialize_serving_record_batch` on the same embedding, with `model: "bge-small"`:
     matrixark_mcp_serving_records returns TWO records, the embedding and a
     `context_model_registry` row built by `context_model_registry_records`.
     matrixark_mcp_core_compact returns one. Its batch function has no such call, and nothing
     else on the backfill or direct-backend path writes that row -- `context_model_registry`
     appears nowhere in either module.

  3. The debug-record gate. matrixark_mcp_core_compact reads a module-level
     `ENABLE_CONTEXT_DEBUG_RECORDS` resolved at import; matrixark_mcp_serving_records calls
     `context_debug_records_enabled()`, which also looks up `matrixark_mcp_core` in `sys.modules`
     at CALL time. Setting the flag on `matrixark_mcp_core` -- which is what
     `run_matrixark_message_pdf_debug_trace` does with its `--include-debug-audit` argument --
     makes the serving_records copy emit the debug record and leaves the core_compact copy
     emitting one record, measured both ways below.

  4. `session_id` on a context_event with a HASHED scope, and the `storage_part` /
     `storage_record_kind` / `storage_options` / `storage_route` fields, also differ. Those are
     NOT new findings: they are `compact_record_scope` and `attach_storage_route`, already
     recorded in the sibling file, showing through the function that calls them. They are
     asserted here anyway, because the sets below are asserted EXACTLY and a list that left them
     out would not be the whole truth about what these two functions return.

  5. `session_id` on a context_event whose scope is ID-ONLY -- `{tenant_id, user_id,
     session_id}`, the documented public shape -- IS this function's own. `canonical_scope_key`
     returns "" for that shape, so `compact_record_scope` takes its early return and leaves
     `scope` on the record; `matrixark_mcp_serving_records.materialize_serving_records` then
     lifts `session_id` off it with an inline block of its own, and
     `matrixark_mcp_core_compact.materialize_serving_records` has no such block.

     That block looks dead and is not, which is why the id-only fixture exists. Deleting it and
     re-running this file with only a hashed-scope fixture changes NOTHING, because by then
     `compact_record_scope` has already lifted the session id and popped the scope. It is the
     id-only scope that reaches it, and there the block is the only thing in either copy that
     can keep the session id at all.

THIS FILE DOES NOT ASSERT THAT THE COPIES AGREE. Making either match the other changes what is
written on an ingest path -- adding a `context_model_registry` row to the direct backend's writes,
or two count fields to the local adapter's embeddings. That is a decision, not a cleanup. So the
divergence is recorded in BOTH directions: a new one fails here, a resolved one fails here too.

NOT CLAIMED: that `run_matrixark_message_pdf_debug_trace`'s own writes take the core_compact path.
What is measured is that the flag it sets reaches one copy and not the other. Which path that tool
drives is a separate question this file does not answer. Nor is anything claimed about whether the
model-registry row is needed on the direct-backend path -- only that one copy emits it and the
other does not.
"""
from __future__ import annotations

import ast
import copy
import importlib
import os
import unittest
from typing import Dict, List, Set, Tuple

TOOLS = os.path.dirname(os.path.abspath(__file__))

FUNCTIONS = ("materialize_serving_records", "materialize_serving_record_batch")

#: Every live copy, and what reaches it. Every assertion is driven from this dict.
LIVE_COPIES: Dict[str, str] = {
    "matrixark_mcp_core_compact":
        "matrixark_context_backfill and matrixark_temporal_direct_backend",
    "matrixark_mcp_serving_records":
        "matrixark_mcp_local_adapter, matrixark_mcp_server and matrixark_mcp_temporal_adapters",
}

#: module that binds the batch entry point -> the copy it resolves to. Checked at runtime.
BATCH_BINDERS: Dict[str, str] = {
    "matrixark_context_backfill": "matrixark_mcp_core_compact",
    "matrixark_temporal_direct_backend": "matrixark_mcp_core_compact",
    "matrixark_mcp_local_adapter": "matrixark_mcp_serving_records",
    "matrixark_mcp_server": "matrixark_mcp_serving_records",
}

#: Keys on the scope. HASHES, not ids: canonical_scope_key returns "" for an id-only scope, so
#: this shape is what reaches the compaction branch.
SCOPE = {"tenant_hash": 111, "user_hash": 222, "session_hash": 333, "agent_hash": 0,
         "tenant_id": "t1", "session_id": "sess-xyz", "user_id": "u1"}

#: The documented public scope shape, which canonical_scope_key answers "" for. It reaches the
#: OTHER branch: `scope` survives compact_record_scope and is still on the record when
#: materialize_serving_records runs, which is the only place either copy can lift a session id
#: from. Without this fixture the inline block in the serving copy looks like dead code.
ID_ONLY_SCOPE = {"tenant_id": "t1", "user_id": "u1", "session_id": "sess-idonly"}

#: Fields one copy produces for a record type and no other copy does. Asserted exactly, in both
#: directions. `session_id`, `storage_part`, `storage_record_kind` and `storage_options` are the
#: sibling file's finding showing through; `source_event_count` and `source_segment_count` are
#: this one's.
RECORDED_ONLY_IN = {
    "context_event": {
        "matrixark_mcp_core_compact": frozenset(),
        "matrixark_mcp_serving_records": frozenset({
            "session_id", "storage_part", "storage_record_kind"}),
    },
    "context_event_id_only_scope": {
        "matrixark_mcp_core_compact": frozenset(),
        "matrixark_mcp_serving_records": frozenset({
            "session_id", "storage_part", "storage_record_kind", "storage_route"}),
    },
    "context_embedding": {
        "matrixark_mcp_core_compact": frozenset({"source_event_count", "source_segment_count"}),
        "matrixark_mcp_serving_records": frozenset({
            "storage_options", "storage_part", "storage_record_kind"}),
    },
}

#: Record types `materialize_serving_record_batch` emits for one embedding carrying a model name.
RECORDED_BATCH_TYPES = {
    "matrixark_mcp_core_compact": ("context_embedding",),
    "matrixark_mcp_serving_records": ("context_embedding", "context_model_registry"),
}

#: Which module's ENABLE_CONTEXT_DEBUG_RECORDS each copy answers to. A copy is listed under a
#: module when setting the flag THERE makes it emit the debug record.
RECORDED_DEBUG_GATE = {
    "matrixark_mcp_core": ("matrixark_mcp_serving_records",),
    "matrixark_mcp_core_compact": ("matrixark_mcp_core_compact",),
    "matrixark_mcp_serving_records": ("matrixark_mcp_serving_records",),
}

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


def _copy(stem: str, function: str):
    return getattr(_import(stem), function)


def _event_record(extra: Dict[str, object] | None = None,
                  scope: Dict[str, object] | None = None) -> Dict[str, object]:
    record = {"record_type": "context_event", "node_hash": 42, "node_path": ["root", "n"],
              "event_id_hash": 7, "timestamp_ms": 1700000000000,
              "scope": dict(scope if scope is not None else SCOPE), "text": "hi"}
    record.update(extra or {})
    return record


def _id_only_event_record() -> Dict[str, object]:
    return _event_record(scope=ID_ONLY_SCOPE)


def _embedding_record() -> Dict[str, object]:
    return {"record_type": "context_embedding", "embedding_type": "node_l0", "ref_type": "node",
            "ref_hash": 9, "node_hash": 42, "scope": dict(SCOPE), "model": "bge-small",
            "source_event_ids": ["e1", "e2", "e3"], "source_segment_hashes": [11, 12],
            "source_session_ids": ["s1"], "source_entity_hashes": [5], "vector": [0.1, 0.2],
            "updated_at_ms": 1700000000000}


FIXTURES = {"context_event": _event_record,
            "context_event_id_only_scope": _id_only_event_record,
            "context_embedding": _embedding_record}

#: Fixtures whose scope canonical_scope_key CAN name, so compact_record_scope compacts them.
COMPACTED_FIXTURES = ("context_event", "context_embedding")


def _fields(stem: str, record_type: str) -> Set[str]:
    """Every field the named copy puts on any record it returns for this fixture."""
    shaped = _copy(stem, "materialize_serving_records")(copy.deepcopy(FIXTURES[record_type]()))
    return {key for item in shaped for key in item}


class TheRecordMaterializerHasOneDefinition(unittest.TestCase):

    def test_the_copies_are_there_to_compare(self) -> None:
        """A floor and the anchor. Every assertion below passes over an empty read."""
        for function in FUNCTIONS:
            found, scanned = _defining_modules(function)
            with self.subTest(function=function):
                self.assertGreaterEqual(
                    scanned, MODULE_SCAN_FLOOR,
                    "the definition scan read only %d production modules" % scanned)
                self.assertEqual(
                    sorted(LIVE_COPIES), sorted(found),
                    "%s is defined by a different set of production modules than this file "
                    "records" % function)
            objects = {stem: _copy(stem, function) for stem in LIVE_COPIES}
            for stem, bound in objects.items():
                with self.subTest(function=function, module=stem):
                    self.assertEqual(
                        stem, bound.__module__.rsplit(".", 1)[-1],
                        "%s.%s is owned by %s" % (stem, function, bound.__module__))
            self.assertEqual(
                len(LIVE_COPIES), len({id(bound) for bound in objects.values()}),
                "the two %s are the same object, so one module delegates to the other and the "
                "split is resolved" % function)

    def test_each_copy_is_on_a_live_path(self) -> None:
        """Runtime binding, not import statements."""
        for stem, expected in BATCH_BINDERS.items():
            with self.subTest(module=stem):
                bound = getattr(_import(stem), "materialize_serving_record_batch", None)
                self.assertIsNotNone(
                    bound, "%s no longer binds materialize_serving_record_batch" % stem)
                self.assertEqual(
                    expected, bound.__module__.rsplit(".", 1)[-1],
                    "%s now reaches a different copy; the split moved under this file" % stem)
        reached = {expected for expected in BATCH_BINDERS.values()}
        self.assertEqual(
            sorted(LIVE_COPIES), sorted(reached),
            "the binders listed here no longer cover every live copy, so one of them could drift "
            "with nothing watching")

    def test_the_fixtures_reach_the_branches_under_test(self) -> None:
        """A floor on the FIXTURE, not the code.

        Both copies return the record untouched when its type is not hot, and neither writes a
        scope_key when canonical_scope_key gives "" -- an id-only scope makes every set below
        agree for the most boring reason.
        """
        hot = _import("matrixark_mcp_core_compact").HOT_SERVING_RECORD_TYPES
        for record_type, build in FIXTURES.items():
            with self.subTest(record_type=record_type):
                self.assertIn(
                    str(build()["record_type"]), hot,
                    "%s is no longer a hot serving type, so both copies return it untouched"
                    % record_type)
        for record_type in COMPACTED_FIXTURES:
            for stem in LIVE_COPIES:
                with self.subTest(record_type=record_type, module=stem):
                    shaped = _copy(stem, "materialize_serving_records")(
                        copy.deepcopy(FIXTURES[record_type]()))
                    self.assertTrue(
                        str(shaped[-1].get("scope_key") or ""),
                        "%s produced no scope_key for the %s fixture, so it took the early return"
                        % (stem, record_type))
        # The id-only fixture must reach the OTHER branch: `scope` still on the record when
        # materialize_serving_records runs. Otherwise the inline lift is never executed and the
        # session_id entry recorded for it would be measuring compact_record_scope again.
        for stem in LIVE_COPIES:
            with self.subTest(module=stem, branch="id-only scope"):
                module = _import(stem)
                partly = module.compact_storage_record(_id_only_event_record())
                self.assertIn(
                    "scope", partly,
                    "%s's compact_storage_record now removes `scope` from an id-only scope too, "
                    "so materialize_serving_records never sees one and the inline lift recorded "
                    "below is not being exercised" % stem)
                self.assertFalse(
                    str(partly.get("scope_key") or ""),
                    "%s now names an id-only scope, so this fixture no longer reaches the branch "
                    "it was written for" % stem)
        embedding = _embedding_record()
        self.assertTrue(
            embedding["source_event_ids"] and embedding["source_segment_hashes"],
            "the embedding fixture no longer carries the lists the count fields are derived from")
        self.assertTrue(
            str(embedding["model"]),
            "the embedding fixture no longer carries a model name, so "
            "context_model_registry_records has nothing to build a row from and the batch "
            "comparison is vacuous")

    def test_the_fields_only_one_copy_writes_are_exactly_these(self) -> None:
        """Asserted as a set, in both directions, for every live copy."""
        for record_type in FIXTURES:
            produced = {stem: _fields(stem, record_type) for stem in LIVE_COPIES}
            for stem in LIVE_COPIES:
                others: Set[str] = set()
                for other, keys in produced.items():
                    if other != stem:
                        others |= keys
                with self.subTest(record_type=record_type, module=stem):
                    self.assertEqual(
                        sorted(RECORDED_ONLY_IN[record_type][stem]),
                        sorted(produced[stem] - others),
                        "%s now writes a different set of %s fields that no other copy writes. "
                        "Added names are a new divergence; missing ones mean a winner was chosen "
                        "and this record should say which." % (stem, record_type))

    def test_the_batch_record_types_are_exactly_these(self) -> None:
        """The consequence with the widest blast radius: a whole record type, or not."""
        for stem, expected in RECORDED_BATCH_TYPES.items():
            batch = _copy(stem, "materialize_serving_record_batch")([_embedding_record()])
            with self.subTest(module=stem):
                self.assertEqual(
                    sorted(expected),
                    sorted({str(item.get("record_type") or "") for item in batch}),
                    "%s.materialize_serving_record_batch now emits a different set of record "
                    "types for one embedding carrying a model name" % stem)
        self.assertNotEqual(
            sorted(RECORDED_BATCH_TYPES["matrixark_mcp_core_compact"]),
            sorted(RECORDED_BATCH_TYPES["matrixark_mcp_serving_records"]),
            "this file records the two batch functions as emitting the same record types, which "
            "would mean there is nothing here to record")

    def test_the_debug_gate_each_copy_answers_to_is_exactly_this(self) -> None:
        """Which module's flag turns the debug record on, per copy. Recorded both ways."""
        debug_fields = sorted(_import("matrixark_mcp_core_compact").EVENT_DEBUG_FIELDS)
        self.assertTrue(debug_fields, "EVENT_DEBUG_FIELDS is empty, so no debug payload exists")
        field = debug_fields[0]

        def emitted() -> Set[str]:
            out = set()
            for stem in LIVE_COPIES:
                shaped = _copy(stem, "materialize_serving_records")(
                    _event_record({field: {"x": 1}}))
                if any(str(item.get("record_type") or "") == "context_debug_record"
                       for item in shaped):
                    out.add(stem)
            return out

        modules = {stem: _import(stem) for stem in RECORDED_DEBUG_GATE}
        originals = {stem: getattr(module, "ENABLE_CONTEXT_DEBUG_RECORDS", False)
                     for stem, module in modules.items()}
        try:
            for stem, module in modules.items():
                module.ENABLE_CONTEXT_DEBUG_RECORDS = False
            self.assertEqual(
                set(), emitted(),
                "a debug record was emitted with every gate off, so the gate comparison below "
                "cannot mean anything")
            for holder, module in modules.items():
                module.ENABLE_CONTEXT_DEBUG_RECORDS = True
                try:
                    with self.subTest(flag_set_on=holder):
                        self.assertEqual(
                            sorted(RECORDED_DEBUG_GATE[holder]), sorted(emitted()),
                            "setting ENABLE_CONTEXT_DEBUG_RECORDS on %s now reaches a different "
                            "set of copies" % holder)
                finally:
                    module.ENABLE_CONTEXT_DEBUG_RECORDS = False
        finally:
            for stem, module in modules.items():
                module.ENABLE_CONTEXT_DEBUG_RECORDS = originals[stem]


if __name__ == "__main__":
    unittest.main()
