#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Every site that serialises a record for storage slims it first.

Four sites turn a record into a persisted entry: the bundle path and the legacy index path in
matrixark_temporal_direct_write, the raw path in matrixark_temporal_direct_backend, and
matrixark_mcp_raw_ingestion. The slimmers were applied at ONE of them, on the strength of a comment
reading "This is the ONLY append call site" -- which is true of the append CLIENT, not of record
serialisation.

Measured on the live log afterwards: of 26,944 records written, **871 still carried the derived
fields** -- 504 of 1,065 matrixark_idempotency rows and 366 of 619 context_summary rows.

test_no_persist_site_serialises_an_unslimmed_record is the point of this file. Applying a slimmer to
one of several equivalent paths is the defect that keeps recurring here, and a structural check is
the only kind that catches the FIFTH site before it ships.
"""
import ast
import os
import unittest

try:
    from tools.matrixark_mcp_temporal_append import slim_persisted_record
except ImportError:  # run from tools/
    from matrixark_mcp_temporal_append import slim_persisted_record

HERE = os.path.dirname(os.path.abspath(__file__))
# matrixark_mcp_temporal_append.py carries a SECOND copy of the append implementation -- the same
# legacy-index and bundle branches as matrixark_temporal_direct_write. Leaving it off this list is
# why five successive changes to what gets slimmed never reached it, and why context_summary was
# still written fat after four of them.
PERSIST_MODULES = (
    "matrixark_temporal_direct_write.py",
    "matrixark_temporal_direct_backend.py",
    "matrixark_mcp_raw_ingestion.py",
    "matrixark_mcp_temporal_append.py",
)


def _payload_assignments(tree):
    """Every `payload = json.dumps(...)`, with whether its function slims anything.

    The check is function-scoped rather than line-literal because the bundle path serialises a
    variable built from an already-slimmed list, which a literal check reads as a bypass when it is
    not one. Requiring the enclosing function to slim still catches what matters: a NEW persist site
    in a function that never slims.
    """
    for fn in ast.walk(tree):
        if not isinstance(fn, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        slims = any(
            isinstance(n, ast.Call) and isinstance(n.func, ast.Name)
            and n.func.id == "slim_persisted_record"
            for n in ast.walk(fn))
        for node in ast.walk(fn):
            if isinstance(node, ast.Assign) \
               and any(isinstance(t, ast.Name) and t.id == "payload" for t in node.targets):
                call = node.value
                if isinstance(call, ast.Call) and isinstance(call.func, ast.Attribute) \
                   and call.func.attr == "dumps":
                    yield fn, node, call, slims
                continue
            # An entry can spell its value inline, with no intermediate variable at all:
            #     {"key": ..., "field": ..., "value": json.dumps(record_copy, ...)}
            # The latest-context-state writers do exactly that, and went unseen by the first
            # version of this guard for that reason alone. A guard that knows one spelling of the
            # idiom is not a guard.
            if isinstance(node, ast.Dict):
                names = {k.value for k in node.keys
                         if isinstance(k, ast.Constant) and isinstance(k.value, str)}
                if "value" not in names or not ({"key", "field"} & names):
                    continue
                for key, value in zip(node.keys, node.values):
                    if not (isinstance(key, ast.Constant) and key.value == "value"):
                        continue
                    if not (isinstance(value, ast.Call)
                            and isinstance(value.func, ast.Attribute)
                            and value.func.attr == "dumps"):
                        continue
                    # Only a RECORD needs slimming. The same entry shape also carries index
                    # postings -- {"ref_hashes": ...}, {"locations": ...} -- which have none of
                    # the fields a slimmer looks for, so flagging those would be noise that
                    # trains the next reader to wave this test through.
                    if value.args and "record" in ast.unparse(value.args[0]):
                        yield fn, node, value, slims


class EveryPersistSiteSlimsTests(unittest.TestCase):
    def test_no_persist_site_serialises_an_unslimmed_record(self):
        checked = 0
        for name in PERSIST_MODULES:
            path = os.path.join(HERE, name)
            if not os.path.exists(path):
                continue
            with open(path, encoding="utf-8") as handle:
                tree = ast.parse(handle.read())
            for fn, node, call, slims in _payload_assignments(tree):
                checked += 1
                self.assertTrue(
                    slims,
                    f"{name}:{node.lineno} in {fn.name}() serialises a record for storage and that "
                    f"function never calls slim_persisted_record:\n    {ast.unparse(call)[:120]}")
        self.assertGreaterEqual(checked, 3,
                                "expected to find the persist sites; the idiom may have moved")

    def test_the_helper_composes_all_three(self):
        record = {
            "record_type": "context_summary",
            "storage_record_kind": "summary",
            "storage_part": "summary",
            "storage_options": {"route": "default", "write_mode": "async",
                                "durability_result": "accepted_for_async_durability",
                                "sync_write": False},
            "scope_key": "t=1|u=2|s=3|",
            "node_hash": 7,
            "summary_text": "kept",
        }
        slim = slim_persisted_record(record)
        self.assertNotIn("storage_record_kind", slim)
        self.assertNotIn("storage_part", slim)
        self.assertNotIn("durability_result", slim["storage_options"])
        self.assertNotIn("sync_write", slim["storage_options"])
        self.assertEqual("kept", slim["summary_text"])

    def test_the_inputs_are_not_dropped(self):
        """It must remain the conservative set -- an input is not a derived field."""
        record = {"storage_options": {"route": "default", "write_mode": "async",
                                      "background_write": False, "read_preference": "primary"}}
        kept = slim_persisted_record(record)["storage_options"]
        for field in ("route", "write_mode", "background_write", "read_preference"):
            self.assertIn(field, kept, f"{field} is an input and must survive")

    def test_it_is_a_no_op_on_a_record_with_none_of_it(self):
        record = {"record_type": "context_event", "text": "hello"}
        self.assertIs(record, slim_persisted_record(record))

    def test_it_does_not_mutate_the_caller(self):
        options = {"route": "default", "durability_result": "accepted_for_async_durability"}
        record = {"storage_record_kind": "summary", "record_type": "context_summary",
                  "storage_options": options}
        slim_persisted_record(record)
        self.assertIn("storage_record_kind", record)
        self.assertIn("durability_result", options)


if __name__ == "__main__":
    unittest.main()
