#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""An exhausted write retry says WHICH write gave up.

`_write_with_backoff(fn, *, op=...)` took an operation name from all six of its callers -- "hset",
"batch_hset", "put_string", "matrixark_batch_append_records" among them -- and the retry loop never
read it. A write that exhausted its retries re-raised the underlying exception and nothing said
which of the six had failed, so the one piece of information the callers had gone to the trouble of
supplying was the one piece the failure did not carry.

This guard exists because that defect is invisible to every other test in the tree: the loop
BEHAVED correctly, it simply said nothing, and no assertion anywhere reads what it says.

It matters more here than the size of the change suggests. The modules that exercise this path --
`test_matrixark_mcp_backend_policy` and `test_backend_policy_part1/2/3`, 136 test methods between
them -- raise `SkipTest` at import in this repository, because `run_matrixark_rust_scale_report` is
absent from it and the first of them imports from it. So this file is deliberately built on the
mixin alone: no scale report, no backend fixture, nothing that would make it skip alongside them.

Four controls, because "a line was logged" is much weaker than it looks:

  * a DIFFERENT op must produce a DIFFERENT line -- otherwise the assertion passes against a
    hardcoded string that never consults `op` at all
  * a write that SUCCEEDS must log nothing -- otherwise an unconditional log satisfies the test
  * a write that fails and then succeeds INSIDE the retry budget must log nothing and return --
    the line belongs to exhaustion, not to any failure
  * the original exception must still arrive, same type, same message, same object -- `raise` is
    bare on purpose and a wrapped exception would be a behaviour change wearing a logging change's
    clothes
"""
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

# Reached through the parent on purpose: matrixark_temporal_direct_write and
# matrixark_mcp_temporal_adapters import each other, so importing the mixin module first
# raises ImportError. Production binds the mixin through the adapter, and so does this.
import matrixark_mcp_temporal_adapters  # noqa: F401
import matrixark_temporal_direct_write as direct_write


class _Writer(direct_write._TemporalDirectWriteMixin):
    """The mixin with only what the retry loop reads, so nothing else can decide the outcome."""

    def __init__(self, retries: int) -> None:
        self._write_retries = retries
        self._write_backoff_s = 0.0


class AnExhaustedWriteRetrySaysWhichWrite(unittest.TestCase):
    def setUp(self) -> None:
        self.logged: list[str] = []
        self._real_log = direct_write._mcp_debug_log
        direct_write._mcp_debug_log = self.logged.append
        self.addCleanup(setattr, direct_write, "_mcp_debug_log", self._real_log)

    @staticmethod
    def _always_raises(exc):
        def fn():
            raise exc
        return fn

    def test_the_line_names_the_operation_that_gave_up(self) -> None:
        writer = _Writer(retries=0)
        with self.assertRaises(RuntimeError):
            writer._write_with_backoff(self._always_raises(RuntimeError("store down")), op="hset")
        self.assertEqual(1, len(self.logged), "exhaustion logged %d lines" % len(self.logged))
        self.assertIn("hset", self.logged[0],
                      "the line does not name the operation: %r" % self.logged[0])

    def test_a_different_operation_gives_a_different_line(self) -> None:
        # Without this, a line that ignores `op` entirely passes the test above.
        writer = _Writer(retries=0)
        for op in ("hset", "put_string"):
            with self.assertRaises(RuntimeError):
                writer._write_with_backoff(self._always_raises(RuntimeError("x")), op=op)
        self.assertEqual(2, len(self.logged))
        self.assertNotEqual(self.logged[0], self.logged[1],
                            "both operations produced the same line, so `op` is not being read")
        self.assertIn("put_string", self.logged[1])
        self.assertNotIn("put_string", self.logged[0])

    def test_a_write_that_succeeds_logs_nothing(self) -> None:
        writer = _Writer(retries=0)
        writer._write_with_backoff(lambda: None, op="hset")
        self.assertEqual([], self.logged, "a successful write logged %r" % self.logged)

    def test_a_write_that_recovers_inside_the_budget_logs_nothing(self) -> None:
        # The line belongs to EXHAUSTION. A failure that the retry absorbs is not a failure.
        attempts = []

        def flaky():
            attempts.append(1)
            if len(attempts) < 3:
                raise RuntimeError("transient")

        writer = _Writer(retries=5)
        writer._write_with_backoff(flaky, op="batch_hset")
        self.assertEqual(3, len(attempts))
        self.assertEqual([], self.logged, "a recovered write logged %r" % self.logged)

    def test_the_original_exception_still_arrives_unchanged(self) -> None:
        original = ValueError("readiness hget readback mismatch")
        writer = _Writer(retries=1)
        with self.assertRaises(ValueError) as caught:
            writer._write_with_backoff(self._always_raises(original), op="put_string")
        self.assertIs(original, caught.exception,
                      "the retry loop replaced the exception instead of re-raising it")
        self.assertEqual("readiness hget readback mismatch", str(caught.exception))


if __name__ == "__main__":
    unittest.main()
