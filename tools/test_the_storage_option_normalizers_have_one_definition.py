#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""`normalize_storage_options` is defined twice and the two copies answer differently.

Both are live, and they sit on the two halves of the same write:

    matrixark_mcp_core.normalize_storage_options
        bound by matrixark_mcp_requests, matrixark_mcp_retrieve_request,
        matrixark_mcp_session_runtime, matrixark_local_adapter_retrieve and
        matrixark_local_adapter_session_commit -- the REQUEST path, which normalises the
        storage_options a caller sent.

    matrixark_mcp_storage_options.normalize_storage_options
        reached through `storage_options_for_record`, which matrixark_mcp_serving_records
        imports -- the PER-RECORD path, which normalises the options riding on each record's
        envelope as it is materialised.

So one request is normalised twice, by two different functions, and they do not agree.

MEASURED BY EXECUTION, not by reading the source: both copies were imported and called on a
matrix of 864 option shapes built from the keys that decide a route. 499 of the 864 came back
different. Three kinds of difference, and the matrix below re-derives all three every run:

  1. VALIDATION. Each copy refuses an input the other accepts, and no input is refused by both
     for different reasons:
       * `route: "default"` -- the request path raises MatrixArkError, the record path accepts
         it (its allowed-value set is `STORAGE_ROUTE_PRESETS | {"default"}`). "default" is not
         in the schema's route enum either way, so this is the record path being the lenient
         one, not the request path being wrong.
       * `background_write: true` alongside a sync durability -- the record path raises, the
         request path accepts and stores the contradiction. The request path DOES raise for the
         same pair spelled `write_mode: "sync"`; it is only the durability spelling that slips
         through, because the record path has an `elif durability in {async, sync}` that
         promotes durability into write_mode and oplog_mode and the request path has none.
       * an out-of-enum `read_preference` -- the record path raises, the request path passes the
         value straight through into the stored options. The schema declares that enum.

  2. THE TOP-LEVEL ALIAS TABLE. The record path's table has 11 entries, the request path's has
     9, and the two extra are `temporalstore_durability` and `temporalstore_read_preference`.
     Executed one alias at a time: the other nine are honoured by both, those two by the record
     path only. The sharpest form of it is in the fixture floor below -- an argument block whose
     ONLY storage option is `temporalstore_durability: "sync"` comes back from the request path
     as `{}`, no storage options at all, while the record path returns a full sync route. With a
     storage_mode already set, the same argument routes raft_async with
     `write_ack_policy: ack_after_memory_append` through one and raft_sync with
     `ack_after_durable_commit` through the other: a caller asking to be durable before ack is
     acknowledged from memory on the request path.

  3. THE MERGED ROUTE FIELDS. `replica_read` is the one key the record path can produce and the
     request path never can.

THIS FILE DOES NOT ASSERT THAT THEY AGREE. Making either copy match the other changes what the
live path accepts, what it rejects, and what is written into `storage_options` on every record --
`test_matrixark_the_derived_storage_options_are_not_stored` is built on the field list one of
them merges. That is a decision about serving behaviour, not a cleanup, so this RECORDS the
divergence in both directions: a new one fails here, and a resolved one fails here too.

Why a second file when `test_the_schema_names_what_the_route_reads` already guards this module
pair: that one compares `canonical_storage_route`, the resolver these two call at their tail. It
is thorough about the resolver and says nothing about the normaliser wrapped around it, which is
where the alias table and every validation rule live. `test_matrixark_python_module_boundaries`
exercises `normalize_storage_options` four times and imports `tools.matrixark_mcp_core` every
time -- including a top-level-alias case and a background_write rejection, both of which pass on
the copy it calls and neither of which is true of the other copy. A guard that covers one of two
live copies leaves the other free to drift, which is what happened here.

NOT CLAIMED: that any caller sets `temporalstore_durability` today. It appears nowhere in this
tree outside the one alias table, and the storage-options schema does not advertise it. What is
claimed is that the two normalisers disagree about it, that both are live, and that the
disagreement is in the direction of acknowledging a durability request from memory.
"""
from __future__ import annotations

import ast
import importlib
import itertools
import os
import unittest
from typing import Dict, Set, Tuple

TOOLS = os.path.dirname(os.path.abspath(__file__))

FUNCTION = "normalize_storage_options"

#: Every live copy, and what reaches it. Every assertion below is driven from this dict rather
#: than from a hand-written list, so a third copy cannot be recorded in one place and missed in
#: another.
LIVE_COPIES: Dict[str, str] = {
    "matrixark_mcp_core":
        "the request path: matrixark_mcp_requests, matrixark_mcp_retrieve_request, "
        "matrixark_mcp_session_runtime, matrixark_local_adapter_retrieve and "
        "matrixark_local_adapter_session_commit all bind it from here",
    "matrixark_mcp_storage_options":
        "the per-record path: storage_options_for_record calls it, and "
        "matrixark_mcp_serving_records imports that",
}

#: Modules that bind the request-path copy directly, checked at runtime rather than by reading
#: their import statements.
REQUEST_PATH_BINDERS = (
    "matrixark_mcp_requests",
    "matrixark_mcp_retrieve_request",
    "matrixark_mcp_session_runtime",
    "matrixark_local_adapter_retrieve",
    "matrixark_local_adapter_session_commit",
)

#: The record path is reached through this function, not by binding the name.
RECORD_PATH_ENTRY = ("matrixark_mcp_serving_records", "storage_options_for_record")

#: Option values that decide a route. The product of these is the matrix.
OPTION_AXES: Dict[str, Tuple[object, ...]] = {
    "route": (None, "default", "raft_sync"),
    "durability": (None, "sync", "async"),
    "write_mode": (None, "sync"),
    "background_write": (None, True),
    "read_preference": (None, "primary", "not_a_value"),
    "storage_mode": (None, "raft"),
}

#: Top-level argument aliases, set beside `storage_options` rather than inside it.
ARGUMENT_AXES: Dict[str, Tuple[object, ...]] = {
    "temporalstore_durability": (None, "sync"),
    "temporalstore_read_preference": (None, "primary"),
}

#: (module whose copy refuses it, the storage option named in the error) -- counted ONLY where
#: the other copy accepted the same input. An option both copies refuse is not a divergence:
#: `storage_options.background_write` is refused by both, on different inputs, and appears below
#: only for the input one of them lets through. Recorded exactly, in both directions.
RECORDED_ASYMMETRIC_REFUSALS = {
    ("matrixark_mcp_core", "storage_options.route"),
    ("matrixark_mcp_storage_options", "storage_options.background_write"),
    ("matrixark_mcp_storage_options", "storage_options.read_preference"),
}

#: Output keys one copy can produce over the whole matrix and the other never does.
RECORDED_ONLY_IN = {
    "matrixark_mcp_core": frozenset(),
    "matrixark_mcp_storage_options": frozenset({"replica_read"}),
}

#: Top-level aliases one copy honours and the other ignores.
RECORDED_ALIASES_ONLY_IN = {
    "matrixark_mcp_core": frozenset(),
    "matrixark_mcp_storage_options": frozenset({"temporalstore_durability",
                                                "temporalstore_read_preference"}),
}

#: A floor on the matrix, not on the code. Well under what is measured today (864 and 499), so
#: this is here to fail when the matrix stops exercising anything, not to pin a number.
MATRIX_FLOOR = 800
DIVERGENCE_FLOOR = 400

#: Production modules the definition scan must have read before its answer means anything.
MODULE_SCAN_FLOOR = 200


def _import(stem):
    try:
        return importlib.import_module("tools." + stem)
    except ImportError:  # Direct script execution from tools/.
        return importlib.import_module(stem)


def _defining_modules() -> Tuple[Set[str], int]:
    """Production tools modules with a TOP-LEVEL def of the function, and the denominator."""
    found: Set[str] = set()
    scanned = 0
    for name in sorted(os.listdir(TOOLS)):
        if not name.endswith(".py") or name.startswith("test_") or name.startswith("__"):
            continue
        scanned += 1
        try:
            with open(os.path.join(TOOLS, name), encoding="utf-8", errors="replace") as handle:
                tree = ast.parse(handle.read())
        except (OSError, SyntaxError):  # pragma: no cover - unparseable modules fail elsewhere
            continue
        for node in tree.body:
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == FUNCTION:
                found.add(name[:-3])
    return found, scanned


def _copies():
    """module stem -> the function object that module DEFINES, checked by __module__."""
    out = {}
    for stem in LIVE_COPIES:
        function = getattr(_import(stem), FUNCTION)
        out[stem] = function
    return out


def _call(function, args):
    try:
        return ("ok", function(dict(args)))
    except Exception as exc:  # MatrixArkError, raised by both copies
        # Everything before " must " / " cannot " is the option being refused.
        return ("refused", str(exc).split(" must ")[0].split(" cannot ")[0])


def _matrix():
    """Every argument block in the product of the axes."""
    option_keys = list(OPTION_AXES)
    argument_keys = list(ARGUMENT_AXES)
    for option_values in itertools.product(*(OPTION_AXES[key] for key in option_keys)):
        for argument_values in itertools.product(*(ARGUMENT_AXES[key] for key in argument_keys)):
            args = {key: value for key, value in zip(argument_keys, argument_values)
                    if value is not None}
            args["storage_options"] = {key: value for key, value
                                       in zip(option_keys, option_values) if value is not None}
            yield args


def _run_matrix():
    """Run every input through every live copy once. Returns what the assertions need."""
    copies = _copies()
    refusals = set()
    ever_refused = set()
    produced = {stem: set() for stem in copies}
    inputs = 0
    diverging = 0
    alias_only_swallowed = 0
    write_mode_disagreements = 0
    for args in _matrix():
        inputs += 1
        results = {stem: _call(function, args) for stem, function in copies.items()}
        for stem, (kind, payload) in results.items():
            if kind == "refused":
                ever_refused.add((stem, payload))
            else:
                produced[stem] |= set(payload)
        kinds = {stem: kind for stem, (kind, _payload) in results.items()}
        if len(set(kinds.values())) > 1:
            # ONE copy refused this input and the other did not. That asymmetry is the finding;
            # an option BOTH copies refuse is not a divergence, and three of the four (copy,
            # option) pairs seen anywhere in this matrix are of that harmless kind.
            for stem, (kind, payload) in results.items():
                if kind == "refused":
                    refusals.add((stem, payload))
            diverging += 1
            continue
        if "refused" in kinds.values():
            continue
        payloads = {stem: payload for stem, (_kind, payload) in results.items()}
        shapes = {stem: sorted(payload.items(), key=lambda item: item[0])
                  for stem, payload in payloads.items()}
        if len({repr(shape) for shape in shapes.values()}) > 1:
            diverging += 1
        if not payloads["matrixark_mcp_core"] and payloads["matrixark_mcp_storage_options"]:
            alias_only_swallowed += 1
        write_modes = {payload.get("write_mode") for payload in payloads.values()}
        if len(write_modes) > 1:
            write_mode_disagreements += 1
    return {
        "refusals": refusals,
        "ever_refused": ever_refused,
        "produced": produced,
        "inputs": inputs,
        "diverging": diverging,
        "alias_only_swallowed": alias_only_swallowed,
        "write_mode_disagreements": write_mode_disagreements,
    }


def _alias_value(target: str):
    if target in {"raft_mode", "background_write"}:
        return True
    if target in {"write_mode", "oplog_mode", "durability"}:
        return "sync"
    if target == "read_preference":
        return "primary"
    if target in {"storage_mode", "replication_mode", "storage_family"}:
        return "raft"
    if target == "route":
        return "raft_sync"
    return "linearizable"


def _alias_tables():
    """Both alias tables, read from source. One is a module constant, the other a local."""
    tables: Dict[str, Dict[str, str]] = {}

    def literal(node):
        return {key.value: value.value for key, value in zip(node.keys, node.values)}

    with open(os.path.join(TOOLS, "matrixark_mcp_storage_options.py"),
              encoding="utf-8", errors="replace") as handle:
        tree = ast.parse(handle.read())
    matches = [node for node in tree.body
               if isinstance(node, ast.Assign)
               and any(isinstance(target, ast.Name) and target.id == "_STORAGE_OPTION_ALIASES"
                       for target in node.targets)]
    if len(matches) != 1:
        raise AssertionError(
            "_STORAGE_OPTION_ALIASES matched %d times at module level in "
            "matrixark_mcp_storage_options, not once. This harness cannot read the alias table it "
            "is comparing, so it must refuse rather than report green." % len(matches))
    tables["matrixark_mcp_storage_options"] = literal(matches[0].value)

    with open(os.path.join(TOOLS, "matrixark_mcp_core.py"),
              encoding="utf-8", errors="replace") as handle:
        tree = ast.parse(handle.read())
    matches = []
    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) or node.name != FUNCTION:
            continue
        for inner in ast.walk(node):
            if isinstance(inner, ast.Assign) and any(
                    isinstance(target, ast.Name) and target.id == "aliases"
                    for target in inner.targets):
                matches.append(inner)
    if len(matches) != 1:
        raise AssertionError(
            "the `aliases` table inside matrixark_mcp_core.%s matched %d times, not once. This "
            "harness cannot read the alias table it is comparing, so it must refuse rather than "
            "report green." % (FUNCTION, len(matches)))
    tables["matrixark_mcp_core"] = literal(matches[0].value)
    return tables


class TwoStorageOptionNormalizers(unittest.TestCase):

    def test_the_copies_are_there_to_compare(self) -> None:
        """A floor, and the anchor. Every assertion below passes over an empty read."""
        found, scanned = _defining_modules()
        self.assertGreaterEqual(
            scanned, MODULE_SCAN_FLOOR,
            "the definition scan read only %d production modules, so its answer about how many "
            "copies exist means nothing" % scanned)
        self.assertEqual(
            sorted(LIVE_COPIES), sorted(found),
            "%s is defined by a different set of production modules than this file records. A "
            "new copy must be added to LIVE_COPIES with what reaches it; a copy that is gone "
            "means the split is resolved and this file should go with it." % FUNCTION)
        copies = _copies()
        for stem, function in copies.items():
            with self.subTest(module=stem):
                self.assertEqual(
                    stem, function.__module__.rsplit(".", 1)[-1],
                    "%s.%s resolves to a copy owned by %s" % (stem, FUNCTION, function.__module__))
        self.assertEqual(
            len(LIVE_COPIES), len({id(function) for function in copies.values()}),
            "the copies are the same object, so one module now delegates to the other and this "
            "file is comparing a function with itself")

    def test_each_copy_is_on_a_live_path(self) -> None:
        """Runtime binding, read off __module__ -- not inferred from import statements."""
        for stem in REQUEST_PATH_BINDERS:
            with self.subTest(module=stem):
                bound = getattr(_import(stem), FUNCTION, None)
                self.assertIsNotNone(bound, "%s no longer binds %s" % (stem, FUNCTION))
                self.assertEqual(
                    "matrixark_mcp_core", bound.__module__.rsplit(".", 1)[-1],
                    "%s now takes %s from somewhere else; the request path has moved"
                    % (stem, FUNCTION))
        entry_module, entry_function = RECORD_PATH_ENTRY
        entry = getattr(_import(entry_module), entry_function, None)
        self.assertIsNotNone(
            entry, "%s no longer binds %s, which is how the record path reaches the second copy"
            % (entry_module, entry_function))
        self.assertEqual(
            "matrixark_mcp_storage_options", entry.__module__.rsplit(".", 1)[-1],
            "%s is no longer the matrixark_mcp_storage_options one" % entry_function)
        with open(os.path.join(TOOLS, "matrixark_mcp_storage_options.py"),
                  encoding="utf-8", errors="replace") as handle:
            tree = ast.parse(handle.read())
        calls = 0
        for node in ast.walk(tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) \
                    and node.name == entry_function:
                for inner in ast.walk(node):
                    if isinstance(inner, ast.Call) and isinstance(inner.func, ast.Name) \
                            and inner.func.id == FUNCTION:
                        calls += 1
        self.assertGreater(
            calls, 0,
            "%s no longer calls %s, so the record path may not reach the second copy at all"
            % (entry_function, FUNCTION))

    def test_the_matrix_reaches_the_branches_under_test(self) -> None:
        """A floor on the FIXTURE.

        Every assertion below is over what this matrix produced. A matrix that stopped reaching
        the branches would agree with every recorded set for the most boring reason.
        """
        measured = _run_matrix()
        self.assertGreaterEqual(
            measured["inputs"], MATRIX_FLOOR,
            "the matrix shrank to %d inputs" % measured["inputs"])
        self.assertGreaterEqual(
            measured["diverging"], DIVERGENCE_FLOOR,
            "only %d of %d inputs came back different, so the recorded sets below are being "
            "compared against almost nothing"
            % (measured["diverging"], measured["inputs"]))
        self.assertGreater(
            measured["alias_only_swallowed"], 0,
            "no input in the matrix produced empty storage options through the request path and "
            "a route through the record path, so the alias finding is not being exercised")
        self.assertGreater(
            measured["write_mode_disagreements"], 0,
            "no input in the matrix produced two different write_modes, so the finding this "
            "file exists for is not being exercised")
        for stem in LIVE_COPIES:
            with self.subTest(module=stem):
                self.assertTrue(
                    measured["produced"][stem],
                    "%s's copy returned nothing at all across the whole matrix" % stem)

    def test_the_option_each_copy_refuses_alone_is_exactly_this(self) -> None:
        """Asserted in BOTH directions, as a set, not as a list of examples."""
        measured = _run_matrix()
        self.assertEqual(
            sorted(RECORDED_ASYMMETRIC_REFUSALS), sorted(measured["refusals"]),
            "the two copies now refuse a different set of storage options than this file "
            "records. Added entries are a new divergence; missing ones mean somebody chose a "
            "winner and this record should say which.")
        self.assertLess(
            len(measured["refusals"]), len(measured["ever_refused"]),
            "every refusal in the matrix is now asymmetric, which would mean the two copies "
            "share no validation rule at all -- check the matrix before trusting the set above")

    def test_the_fields_only_one_copy_produces_are_exactly_these(self) -> None:
        measured = _run_matrix()
        produced = measured["produced"]
        for stem in LIVE_COPIES:
            others = set()
            for other, keys in produced.items():
                if other != stem:
                    others |= keys
            with self.subTest(module=stem):
                self.assertEqual(
                    sorted(RECORDED_ONLY_IN[stem]), sorted(produced[stem] - others),
                    "%s's copy now produces a different set of fields no other copy does" % stem)

    def test_the_aliases_only_one_copy_honours_are_exactly_these(self) -> None:
        """Executed one alias at a time, so a table entry that does not take is not counted."""
        tables = _alias_tables()
        self.assertEqual(
            sorted(LIVE_COPIES), sorted(tables),
            "an alias table was not found for every live copy")
        for stem, table in tables.items():
            with self.subTest(module=stem):
                self.assertGreater(len(table), 5, "%s's alias table read as %d entries, which is "
                                                  "too few to be the real one" % (stem, len(table)))
        copies = _copies()
        every_alias = {alias: target for table in tables.values() for alias, target in table.items()}
        honoured = {stem: set() for stem in copies}
        for alias, target in sorted(every_alias.items()):
            args = {alias: _alias_value(target)}
            for stem, function in copies.items():
                kind, payload = _call(function, args)
                if kind == "ok" and payload:
                    honoured[stem].add(alias)
        for stem in LIVE_COPIES:
            others = set()
            for other, aliases in honoured.items():
                if other != stem:
                    others |= aliases
            with self.subTest(module=stem):
                self.assertEqual(
                    sorted(RECORDED_ALIASES_ONLY_IN[stem]), sorted(honoured[stem] - others),
                    "%s's copy now honours a different set of top-level aliases that no other "
                    "copy honours" % stem)
        shared = set.intersection(*honoured.values())
        self.assertTrue(
            shared,
            "the two copies no longer honour a single alias in common, which would mean the "
            "tables have nothing to do with each other and this comparison is meaningless")


if __name__ == "__main__":
    unittest.main()
