#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A row key built from a field nobody writes is always None, and always-None looks unique.

`latest_value_record_key` keyed a `resource_import_task` row on `resource_import_task_hash`. Every
writer of that row writes `task_hash`. So the key was `(record_type, None)` for every such row;
`compact_latest_value_records` declines to compact a key with an empty part, on purpose, so that an
absent hash cannot collide with another absent one; and the rows were never superseded. They
accumulated, three per attachment, and nothing reported anything -- a slot that is always None is
indistinguishable from a slot that is always unique until you go and count the rows.

`matrixark_mcp_local_adapter` has its own copy of this function and fixed it there. The copy
`matrixark_mcp_recovery` imports was not carried along, which is this tree's recurring shape: a
helper is extracted, the original keeps evolving, and the extraction quietly becomes the copy that
is wrong.

The check is per SLOT, not per field. `record.get("node_hash") or record.get("node_id")` reads
`node_id`, which no writer of a `context_index` row writes -- but the slot still resolves, because
`node_hash` does. A dead fallback inside a slot is not a defect; a slot with no live source is.
That distinction is the whole check: asked per field it reports four things, three of which are
fine, and an alarm that is mostly wrong gets turned off.

`ARevertedFixIsCaughtTest` runs the check against a copy of the source with the fix undone, and
requires it to name that exact slot. A checker that reports zero without proving it can find one is
reporting the same thing as a checker that has stopped working.
"""
from __future__ import annotations

import ast
import glob
import io
import os
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))

#: Functions that turn a record into its identity. Matched by name, because they are written as
#: free functions in several modules rather than behind one interface.
KEY_FUNCTION_MARKS = ("record_key", "_key_by_type", "record_identity")

#: Two key functions, twelve record types each, today. Floors, so a check that stops FINDING
#: them fails here rather than passing with nothing to say.
KEY_FUNCTIONS_FLOOR = 2
KEYED_RECORD_TYPES_FLOOR = 10


def live_modules() -> list:
    return sorted(path for path in glob.glob(os.path.join(TOOLS, "*.py"))
                  if not os.path.basename(path).startswith("test_"))


def source_of(path: str) -> str:
    with io.open(path, encoding="utf-8", errors="replace") as handle:
        return handle.read()


def _string_keys(node: ast.Dict) -> set:
    return {key.value for key in node.keys
            if isinstance(key, ast.Constant) and isinstance(key.value, str)}


def fields_written_per_record_type(sources: dict) -> dict:
    """record_type -> every field name some writer of that record puts on it."""
    written = {}
    late = set()
    for text in sources.values():
        try:
            tree = ast.parse(text)
        except SyntaxError:  # pragma: no cover - the tree parses
            continue
        for node in ast.walk(tree):
            if isinstance(node, ast.Dict):
                keys = _string_keys(node)
                if "record_type" not in keys:
                    continue
                for key, value in zip(node.keys, node.values):
                    if (isinstance(key, ast.Constant) and key.value == "record_type"
                            and isinstance(value, ast.Constant)
                            and isinstance(value.value, str)):
                        written.setdefault(value.value, set()).update(keys)
            # `record["x"] = ...` is a write too, and which record it lands on is not decidable
            # here -- so it counts for every type. That can only SUPPRESS a finding, never invent
            # one, which is the right direction for a check that authorises leaving code alone.
            if isinstance(node, ast.Assign):
                for target in node.targets:
                    if (isinstance(target, ast.Subscript)
                            and isinstance(target.slice, ast.Constant)
                            and isinstance(target.slice.value, str)):
                        late.add(target.slice.value)
    for record_type in written:
        written[record_type] |= late
    return written


def _fields_read(node: ast.AST) -> set:
    """Field names this expression pulls off the record."""
    found = set()
    for sub in ast.walk(node):
        if (isinstance(sub, ast.Call) and isinstance(sub.func, ast.Attribute)
                and sub.func.attr == "get" and sub.args
                and isinstance(sub.args[0], ast.Constant)
                and isinstance(sub.args[0].value, str)):
            found.add(sub.args[0].value)
    return found


def key_slots(sources: dict) -> list:
    """(module, function, record_type, line, fields) for every slot of every key function."""
    out = []
    for path, text in sources.items():
        stem = os.path.basename(path)[:-3]
        try:
            tree = ast.parse(text)
        except SyntaxError:  # pragma: no cover
            continue
        for node in ast.walk(tree):
            if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            if not any(mark in node.name for mark in KEY_FUNCTION_MARKS):
                continue
            for sub in ast.walk(node):
                if not isinstance(sub, ast.If):
                    continue
                test = sub.test
                if not (isinstance(test, ast.Compare) and len(test.comparators) == 1
                        and isinstance(test.comparators[0], ast.Constant)
                        and isinstance(test.comparators[0].value, str)):
                    continue
                record_type = test.comparators[0].value
                for inner in ast.walk(ast.Module(body=sub.body, type_ignores=[])):
                    if not (isinstance(inner, ast.Return)
                            and isinstance(inner.value, ast.Tuple)):
                        continue
                    for index, element in enumerate(inner.value.elts):
                        if index == 0:
                            continue          # the discriminator, not a looked-up field
                        fields = _fields_read(element)
                        if fields:
                            out.append((stem, node.name, record_type,
                                        getattr(element, "lineno", inner.lineno), fields))
    return out


def dead_slots(sources: dict) -> list:
    """Slots whose every source field is written by no writer of that record type."""
    written = fields_written_per_record_type(sources)
    dead = []
    for stem, name, record_type, line, fields in key_slots(sources):
        if record_type not in written:
            continue                          # no writer found at all: a different question
        if not (fields & written[record_type]):
            dead.append((stem, name, record_type, line, sorted(fields)))
    return dead


def _sources() -> dict:
    return {path: source_of(path) for path in live_modules()}


class ARowKeySlotResolvesTest(unittest.TestCase):

    def test_no_key_slot_is_always_none(self) -> None:
        dead = dead_slots(_sources())
        self.assertEqual(
            [], dead,
            "a key slot reads only fields nothing writes, so it is None for every row and "
            "compaction declines the key: %s"
            % "; ".join("%s.%s %s at :%d reads %s" % (m, f, t, l, r) for m, f, t, l, r in dead))

    def test_the_check_still_finds_the_key_functions(self) -> None:
        """The floor. Nothing above can fail once this stops finding anything to look at."""
        slots = key_slots(_sources())
        names = {(module, function) for module, function, _t, _l, _f in slots}
        types = {record_type for _m, _f2, record_type, _l, _f in slots}
        self.assertGreaterEqual(
            len(names), KEY_FUNCTIONS_FLOOR,
            "found %d key functions, expected at least %d" % (len(names), KEY_FUNCTIONS_FLOOR))
        self.assertGreaterEqual(
            len(types), KEYED_RECORD_TYPES_FLOOR,
            "found %d keyed record types, expected at least %d"
            % (len(types), KEYED_RECORD_TYPES_FLOOR))

    def test_writers_are_found_for_the_types_that_are_keyed(self) -> None:
        """A type with no writer found is invisible to the check above, so say how many."""
        written = fields_written_per_record_type(_sources())
        types = {record_type for _m, _f, record_type, _l, _fl in key_slots(_sources())}
        unknown = sorted(types - set(written))
        self.assertEqual(
            [], unknown,
            "these keyed record types have no writer this check can see, so their slots are "
            "never checked: %s" % ", ".join(unknown))


class ARevertedFixIsCaughtTest(unittest.TestCase):
    """The floor that matters: undo the fix in a copy and require the check to name that slot."""

    FIXED = ('record.get("task_hash")\n'
             '            if record.get("task_hash") is not None\n'
             '            else record.get("resource_import_task_hash"),')
    BROKEN = 'record.get("resource_import_task_hash"),'

    def test_the_original_defect_would_be_caught(self) -> None:
        path = os.path.join(TOOLS, "matrixark_mcp_latest_values.py")
        sources = _sources()
        self.assertIn(path, sources, "the module moved; re-aim this check")
        self.assertIn(self.FIXED, sources[path],
                      "the fixed shape is gone; this control is no longer testing the fix")

        sources[path] = sources[path].replace(self.FIXED, self.BROKEN)
        dead = dead_slots(sources)
        caught = [(module, record_type, fields) for module, _f, record_type, _l, fields in dead
                  if record_type == "resource_import_task"]
        self.assertTrue(
            caught,
            "the check did not catch the defect it was written for, so its verdict on the real "
            "tree is not evidence")
        self.assertEqual([["resource_import_task_hash"]], [fields for _m, _t, fields in caught])

    def test_a_dead_fallback_beside_a_live_one_is_not_flagged(self) -> None:
        """`node_hash or node_id` reads a field nobody writes and is FINE -- the slot resolves.

        Asked per field this check reports four things, three of them fine. Asked per slot it
        reports the one. An alarm that is mostly wrong is an alarm that gets ignored.
        """
        sources = _sources()
        slots = key_slots(sources)
        fallbacks = [s for s in slots if "node_id" in s[4] and "node_hash" in s[4]]
        self.assertTrue(fallbacks, "the node_hash/node_id slot is gone; re-aim this check")
        written = fields_written_per_record_type(sources)
        for _module, _function, record_type, _line, fields in fallbacks:
            self.assertTrue(fields & written.get(record_type, set()),
                            "this slot should still resolve through its live field")


if __name__ == "__main__":
    unittest.main()
