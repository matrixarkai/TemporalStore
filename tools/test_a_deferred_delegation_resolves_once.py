#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A deferred delegation resolves once, not on every call.

``benchmark_quality_index_terms`` forwards to ``matrixark_mcp_indexing``. The import is deferred on
purpose -- doing it at module scope closes a cycle -- but it was inside the function body, and the
DOTTED form raises ``ModuleNotFoundError`` whenever there is no ``tools`` package on the path, which
is how the adapter runs.

**Python does not cache the fact that a module could not be found.** So every call re-ran the whole
finder: a ``sys.path`` walk, a directory stat per entry, the exception, and only then the fallback
that succeeds out of ``sys.modules``. Measured: a failing ``from tools.X import y`` costs 238.49 us
against 1.45 us for the fallback -- **165x** -- and one post-write retrieve made 307 of them, 75.6 ms
of a 563 ms call.

Counting ATTEMPTS is the test, not timing: the cost is per attempt and the wall clock on a shared
box is not stable enough to see 13% reliably.
"""
from __future__ import annotations

import builtins
import os
import sys
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, TOOLS)

import matrixark_mcp_server  # noqa: F401,E402  -- core <-> core_packing only resolves this way
import matrixark_mcp_core as core  # noqa: E402
import matrixark_mcp_indexing as indexing  # noqa: E402


class ADeferredDelegationResolvesOnceTest(unittest.TestCase):

    def _attempts(self, fn, *, prefix="tools.") -> int:
        """How many imports naming `prefix` are attempted while `fn` runs."""
        count = 0
        real = builtins.__import__

        def counting(name, globals=None, locals=None, fromlist=(), level=0):
            nonlocal count
            if name.startswith(prefix):
                count += 1
            return real(name, globals, locals, fromlist, level)

        builtins.__import__ = counting
        try:
            fn()
        finally:
            builtins.__import__ = real
        return count

    def test_repeated_calls_do_not_re_attempt_the_import(self) -> None:
        """The whole point. One resolution, however many calls follow."""
        core.benchmark_quality_index_terms("warm")          # resolve first
        attempts = self._attempts(
            lambda: [core.benchmark_quality_index_terms(f"value {i}") for i in range(50)])
        self.assertEqual(attempts, 0,
                         "a resolved delegation re-attempted the import that already failed once")

    def test_the_wrapper_answers_what_the_implementation_answers(self) -> None:
        """Memoising a delegate is only correct if it is the same delegate."""
        for args in ((), ("alpha",), ("alpha", "beta"), ("Alpha Beta", "gamma"), (None,), ("",)):
            self.assertEqual(core.benchmark_quality_index_terms(*args),
                             indexing.benchmark_quality_index_terms(*args),
                             f"the wrapper and the implementation disagree on {args!r}")

    def test_it_still_resolves_from_a_cold_module_state(self) -> None:
        """Clearing what it remembered must send it back to the import, not to None."""
        remembered = core._shared_benchmark_quality_index_terms
        try:
            core._shared_benchmark_quality_index_terms = None
            first = self._attempts(lambda: core.benchmark_quality_index_terms("cold"))
            self.assertGreaterEqual(first, 0)          # it may resolve by either arm
            self.assertIsNotNone(core._shared_benchmark_quality_index_terms,
                                 "the delegate was not remembered after a cold resolve")
            again = self._attempts(lambda: core.benchmark_quality_index_terms("warm again"))
            self.assertEqual(again, 0, "the second call re-attempted the import")
        finally:
            core._shared_benchmark_quality_index_terms = remembered

    def test_a_failing_dotted_import_really_is_not_cached(self) -> None:
        """The premise, asserted rather than assumed: this is why the fix is needed at all.

        If Python ever started remembering a failed import, this test would fail and the whole
        change would be unnecessary -- which is worth knowing.
        """
        def attempt():
            try:
                from tools.matrixark_mcp_indexing import benchmark_quality_index_terms  # noqa: F401
            except ModuleNotFoundError:
                pass
        first = self._attempts(attempt)
        second = self._attempts(attempt)
        if first == 0:
            self.skipTest("a `tools` package is importable here, so the dotted form succeeds")
        self.assertEqual(second, first,
                         "a failed import appears to be cached; the deferred shims cost nothing")


if __name__ == "__main__":
    unittest.main(verbosity=2)
