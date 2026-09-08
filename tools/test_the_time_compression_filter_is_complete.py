# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""The time-compression filter must name every record type the method it feeds actually reads.

`refresh_dirty_node_summaries` calls `auto_time_compress_node_events` once per dirty node and used
to hand it the whole live view each time. It now filters once with `time_compression_source_records`.

This guard exists because the obvious reading of that method is WRONG. Its first loop tests one type
in the loop guard and a second one INSIDE the body:

    for record in records:
        if record.get("record_type") != "context_compression_event":
            if record.get("record_type") == "context_recall_reinforcement":
                ...

A filter built from the loop guards alone names two types and silently drops the 1,886
`context_recall_reinforcement` rows -- no error, no failing assertion, just a quieter answer. That
mistake was actually made while writing this change and caught by re-deriving the set from every
comparison in the method, which is what the test below does.

Derived, not listed: a test repeating the four names would pass unchanged after a fifth was added.
"""
from __future__ import annotations

import ast
import os
import unittest

TOOLS_DIR = os.path.dirname(os.path.abspath(__file__))
METHOD_FILE = os.path.join(TOOLS_DIR, "matrixark_local_adapter_summaries.py")
FILTER_FILE = os.path.join(TOOLS_DIR, "matrixark_mcp_summary_runtime.py")
METHOD = "auto_time_compress_node_events"
FILTER = "time_compression_source_records"
CONSTANT = "TIME_COMPRESSION_RECORD_TYPES"


def _parse(path):
    with open(path, encoding="utf-8") as handle:
        return ast.parse(handle.read())


def _named(tree, name):
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == name:
            return node
    return None


def _record_types_compared(fn):
    """Every string compared against a record_type lookup, ANYWHERE in the function.

    Walking the whole body, not just each loop's leading guard -- the nested comparison is the one
    that makes this method easy to read wrongly.
    """
    found = set()
    for node in ast.walk(fn):
        if not isinstance(node, ast.Compare):
            continue
        if "record_type" not in ast.unparse(node):
            continue
        for part in ast.walk(node):
            if isinstance(part, ast.Constant) and isinstance(part.value, str) \
                    and part.value != "record_type":
                found.add(part.value)
    return found


def _constant_value(tree):
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(
                isinstance(t, ast.Name) and t.id == CONSTANT for t in node.targets):
            for sub in ast.walk(node.value):
                if isinstance(sub, ast.Set):
                    return {e.value for e in sub.elts
                            if isinstance(e, ast.Constant) and isinstance(e.value, str)}
    return None


class TheTimeCompressionFilterIsCompleteTest(unittest.TestCase):

    def test_the_scan_finds_the_method_and_its_comparisons(self):
        """A scan matching nothing would report the filter complete."""
        fn = _named(_parse(METHOD_FILE), METHOD)
        self.assertIsNotNone(fn, "%s is gone; the filter it protects has no owner" % METHOD)
        compared = _record_types_compared(fn)
        self.assertGreaterEqual(
            len(compared), 3,
            "found %d record_type comparisons in %s, expected at least 3 -- the scan stopped "
            "matching, so the check below proves nothing" % (len(compared), METHOD))

    def test_the_nested_comparison_is_seen(self):
        """Pin the specific reading error this guard exists for."""
        compared = _record_types_compared(_named(_parse(METHOD_FILE), METHOD))
        self.assertIn(
            "context_recall_reinforcement", compared,
            "the scan missed the type tested INSIDE the first loop's body -- that is precisely the "
            "reading that produces a filter dropping every reinforcement row")

    def test_the_filter_names_every_type_the_method_reads(self):
        declared = _constant_value(_parse(FILTER_FILE))
        self.assertIsNotNone(declared, "%s is not a set literal any more" % CONSTANT)
        compared = _record_types_compared(_named(_parse(METHOD_FILE), METHOD))
        missing = compared - declared
        self.assertEqual(
            set(), missing,
            "%s reads record types %s does not pass through: %s. A caller filtering with it would "
            "hand the method a view with none of those rows in it, and the only symptom would be a "
            "quieter answer." % (METHOD, FILTER, sorted(missing)))

    def test_the_filter_passes_exactly_those_types(self):
        try:  # package path
            from tools.matrixark_mcp_summary_runtime import (
                TIME_COMPRESSION_RECORD_TYPES,
                time_compression_source_records,
            )
        except ImportError:  # Direct script execution from tools/.
            from matrixark_mcp_summary_runtime import (
                TIME_COMPRESSION_RECORD_TYPES,
                time_compression_source_records,
            )
        wanted = sorted(TIME_COMPRESSION_RECORD_TYPES)
        rows = [{"record_type": t} for t in wanted]
        rows += [{"record_type": "context_segment"}, {"record_type": "context_summary"}, {}]
        kept = time_compression_source_records(rows)
        self.assertEqual(wanted, sorted(r["record_type"] for r in kept))

    def test_the_callers_filter_before_the_loop_not_inside_it(self):
        """Filtering inside the per-node loop would cost exactly what this saves."""
        for name in ("matrixark_mcp_summary_runtime.py", "matrixark_local_adapter_summaries.py"):
            tree = _parse(os.path.join(TOOLS_DIR, name))
            calls = [n for n in ast.walk(tree)
                     if isinstance(n, ast.Call)
                     and (getattr(n.func, "id", None) or getattr(n.func, "attr", None)) == FILTER]
            self.assertTrue(calls, "%s no longer filters at all" % name)
            for call in calls:
                inside = [lp for lp in ast.walk(tree)
                          if isinstance(lp, (ast.For, ast.While))
                          and lp.lineno < call.lineno <= (lp.end_lineno or call.lineno)]
                self.assertEqual(
                    [], inside,
                    "%s:%d calls %s inside a loop -- it is meant to run once per refresh"
                    % (name, call.lineno, FILTER))


if __name__ == "__main__":
    unittest.main()
