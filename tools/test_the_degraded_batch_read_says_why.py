#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A batched read that degrades has to say WHICH value poisoned it.

`batch_hget` covers a chunk of 1000 record locations. One value it cannot decode makes the whole
call raise, and the chunk falls back to reading its thousand records one at a time. The live debug
log carries sixty of these, every one of them 1 chunk of 1000, so every one cost a thousand round
trips -- and the line named the cost and not the cause, while the exception sat in hand at the
call site.

The call site comment already knows why that matters: the whole-store fallback used to be silent,
and a 20x slowdown ran unnoticed for as long as one bad value sat in the store. Saying it out loud
without saying why leaves the reader knowing a chunk is poisoned but not which one.
"""
from __future__ import annotations

import ast
import os
import pathlib
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import matrixark_mcp_core as core  # noqa: E402
import matrixark_mcp_temporal_adapters  # noqa: E402,F401  (the retrieve mixin imports circularly)
import matrixark_temporal_direct_retrieve as retrieve  # noqa: E402

# The same file is importable TWICE -- as `matrixark_mcp_core` from tools/, and as
# `tools.matrixark_mcp_core` from the repo root -- and the two are different module objects with
# different attributes. `_batch_hget_degraded_log` resolves the logger at call time and tries the
# PACKAGE path first, so patching only the script-path module leaves the real logger in place and
# this file captures nothing. It then passes or fails according to which path the runner used
# rather than according to the code, which is what happened: green from tools/, three failures
# under the suite.
_CORE_MODULES = [core]
try:  # available whenever the repo root is importable, which is how the suite runs
    from tools import matrixark_mcp_core as _core_package  # noqa: E402
except ImportError:
    pass
else:
    if _core_package is not core:
        _CORE_MODULES.append(_core_package)


class _Captured:
    def __init__(self) -> None:
        self.lines: list[str] = []
        self._original = []

    def __enter__(self) -> "_Captured":
        self._original = [(module, module._mcp_debug_log) for module in _CORE_MODULES]
        for module in _CORE_MODULES:
            module._mcp_debug_log = self.lines.append
        return self

    def __exit__(self, *exc) -> None:
        for module, original in self._original:
            module._mcp_debug_log = original


class TheDegradedBatchReadSaysWhyTest(unittest.TestCase):

    def test_the_cause_reaches_the_log(self) -> None:
        with _Captured() as cap:
            retrieve._batch_hget_degraded_log(
                1, 1000, ["UnicodeDecodeError: invalid start byte at 7"])
        self.assertEqual(1, len(cap.lines))
        self.assertIn("1000 records read one at a time", cap.lines[0])
        self.assertIn("UnicodeDecodeError: invalid start byte at 7", cap.lines[0],
                      "the exception is what identifies the poisoned chunk: %s" % cap.lines[0])

    def test_the_line_is_unchanged_when_there_is_no_cause_to_give(self) -> None:
        """Callers that have nothing to add must not start emitting an empty causes clause."""
        with _Captured() as cap:
            retrieve._batch_hget_degraded_log(2, 1000)
        self.assertNotIn("causes", cap.lines[0])
        self.assertTrue(cap.lines[0].endswith("read one at a time"), cap.lines[0])

    def test_repeated_causes_collapse_and_the_rest_are_counted(self) -> None:
        """One poisoned value raises the same way on every chunk, so the line must not repeat it
        a thousand times -- and it must still say that more kinds were seen."""
        with _Captured() as cap:
            retrieve._batch_hget_degraded_log(
                4, 1000, ["A: x", "A: x", "B: y", "C: z", "D: w"])
        line = cap.lines[0]
        self.assertEqual(1, line.count("A: x"), line)
        self.assertIn("and 1 more", line)


    def test_the_call_site_actually_passes_the_reasons(self) -> None:
        """The helper can format a cause it is never handed.

        Found by mutation: deleting the third argument at the call site left every other test in
        this file passing while the live log went back to naming the cost and not the cause. So the
        WIRING is asserted here, structurally, rather than assumed from the helper being correct.
        """
        source = pathlib.Path(retrieve.__file__).read_text(encoding="utf-8")
        calls = [
            node for node in ast.walk(ast.parse(source))
            if isinstance(node, ast.Call)
            and getattr(node.func, "id", getattr(node.func, "attr", "")) == "_batch_hget_degraded_log"
        ]
        self.assertTrue(calls, "the degradation is no longer reported at all")
        for call in calls:
            self.assertGreaterEqual(
                len(call.args), 3,
                "the call at line %d drops the collected causes, so the log says how much it cost "
                "and not which value did it" % call.lineno)


if __name__ == "__main__":
    unittest.main()
