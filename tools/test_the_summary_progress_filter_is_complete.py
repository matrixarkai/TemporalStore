# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""The caller-side filter must name every record type the function it feeds actually reads.

`refresh_dirty_node_summaries` calls `async_summary_progress_records` once per dirty node, and used
to hand it the whole live view each time -- 7,085 records to reach the 951 it can act on, per node.
It now filters once with `summary_progress_source_records`.

That is only correct while the filter's type set is COMPLETE. If someone teaches the function to
read a third record type, the filter silently removes those rows and the function sees none of
them: no error, no failing assertion anywhere else, just a quieter answer. So the set is derived
here from the function's own source and compared with the constant.

Deriving rather than listing is the point. A test that repeats the two names would pass unchanged
after a third was added -- it would be checking its own copy, not the code.
"""
from __future__ import annotations

import ast
import os
import unittest

TOOLS_DIR = os.path.dirname(os.path.abspath(__file__))
RUNTIME = os.path.join(TOOLS_DIR, "matrixark_mcp_summary_runtime.py")
FUNCTION = "async_summary_progress_records"
FILTER = "summary_progress_source_records"
CONSTANT = "SUMMARY_PROGRESS_RECORD_TYPES"


def _module():
    with open(RUNTIME, encoding="utf-8") as handle:
        return ast.parse(handle.read())


def _function(tree, name):
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == name:
            return node
    return None


def _record_types_compared(fn):
    """Every string a `record_type` lookup is compared against, inside `fn`."""
    found = set()
    for node in ast.walk(fn):
        if not isinstance(node, ast.Compare):
            continue
        text = ast.unparse(node)
        if "record_type" not in text:
            continue
        for part in ast.walk(node):
            if isinstance(part, ast.Constant) and isinstance(part.value, str):
                if part.value != "record_type":
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


class TheSummaryProgressFilterIsCompleteTest(unittest.TestCase):

    def test_the_scan_finds_the_function_and_its_comparisons(self):
        """Without this, a scan that matched nothing would report the filter complete."""
        fn = _function(_module(), FUNCTION)
        self.assertIsNotNone(fn, "%s is gone; the filter it protects has no owner" % FUNCTION)
        compared = _record_types_compared(fn)
        self.assertGreaterEqual(
            len(compared), 2,
            "found %d record_type comparisons in %s, expected at least 2 -- the scan stopped "
            "matching, so the check below proves nothing" % (len(compared), FUNCTION))

    def test_the_filter_names_every_type_the_function_reads(self):
        tree = _module()
        declared = _constant_value(tree)
        self.assertIsNotNone(declared, "%s is not a set literal any more" % CONSTANT)
        compared = _record_types_compared(_function(tree, FUNCTION))
        missing = compared - declared
        self.assertEqual(
            set(), missing,
            "%s reads record types the filter does not pass through: %s. A caller filtering with "
            "%s would hand it a view with none of those rows in it, and the only symptom would be "
            "a quieter answer." % (FUNCTION, sorted(missing), FILTER))

    def test_the_filter_passes_exactly_those_types(self):
        """The behaviour, not the spelling -- the filter could be right and do something else."""
        try:  # package path
            from tools.matrixark_mcp_summary_runtime import (
                SUMMARY_PROGRESS_RECORD_TYPES,
                summary_progress_source_records,
            )
        except ImportError:  # Direct script execution from tools/.
            from matrixark_mcp_summary_runtime import (
                SUMMARY_PROGRESS_RECORD_TYPES,
                summary_progress_source_records,
            )

        wanted = sorted(SUMMARY_PROGRESS_RECORD_TYPES)
        rows = [{"record_type": t} for t in wanted]
        rows += [{"record_type": "context_segment"}, {"record_type": "context_index"}, {}]
        kept = summary_progress_source_records(rows)
        self.assertEqual(wanted, sorted(r["record_type"] for r in kept))
        self.assertEqual(len(SUMMARY_PROGRESS_RECORD_TYPES), len(kept),
                         "the filter dropped or duplicated a type it declares")

    def test_the_callers_filter_before_the_loop_not_inside_it(self):
        """Filtering inside the per-node loop would cost what it was meant to save."""
        for name in ("matrixark_mcp_summary_runtime.py", "matrixark_local_adapter_summaries.py"):
            path = os.path.join(TOOLS_DIR, name)
            with open(path, encoding="utf-8") as handle:
                tree = ast.parse(handle.read())
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
                    "%s:%d calls %s inside a loop -- it is meant to run once per refresh, not "
                    "once per node" % (name, call.lineno, FILTER))


if __name__ == "__main__":
    unittest.main()
