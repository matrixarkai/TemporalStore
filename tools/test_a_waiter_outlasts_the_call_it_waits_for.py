#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A caller queued on a proxy lane must be willing to wait longer than the call ahead of it.

Two timeouts govern one lane. A call holding the lane blocks in `_read_json_line` until its own
deadline; a caller queued behind it blocks in `semaphore.acquire(timeout=_backpressure_timeout_s)`.
They were derived from different numbers:

    holder   max(2.0, request_timeout_ms / 1000 + 2.0)      <- request timeout PLUS two seconds
    waiter   request_timeout_ms / 1000                      <- request timeout

The waiter always gave up exactly 2.0 s before the holder's own deadline, at every timeout. So a
call that ran to its deadline was **guaranteed** to reject every caller queued behind it -- not
under load, by construction. Worse, the error says the lane is under backpressure:

    rejected by pack proxy lane backpressure after 40.000s with 4 workers

which reads as a saturated lane and sends the reader to lane counts and worker pools. The actual
condition is one slow call, and the queue behind it could never have been admitted.

Both now derive from `lane_response_deadline_s`, which is the only place the grace period is
written. This file pins the relationship rather than either number: a future change to the grace,
the floor, or the default is free, as long as a waiter can still outlast one holder.

An operator value still wins, in both spellings, and a blank one is not a value -- exporting
`MATRIXARK_RUST_PROXY_BACKPRESSURE_TIMEOUT_MS=` used to raise `ValueError` out of `int("")` at
client construction.
"""
from __future__ import annotations

import os
import sys
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
if TOOLS not in sys.path:
    sys.path.insert(0, TOOLS)

import matrixark_mcp_rust_proxy_config as config  # noqa: E402

#: Spread deliberately: the floor case (tiny), the transport defaults, and the value whose holder
#: deadline is the 40 s that appeared in the wedge report.
TIMEOUTS_MS = (0, 1, 250, 2000, 5000, 10000, 30000, 38000, 60000, 120000)

BACKPRESSURE_VARS = (
    "MATRIXARK_RUST_PROXY_BACKPRESSURE_TIMEOUT_MS",
    "MATRIXARK_RUST_GATEWAY_BACKPRESSURE_TIMEOUT_MS",
)


class _Target:
    pass


def waiter_seconds(request_timeout_ms: int) -> float:
    target = _Target()
    config.initialize_rust_proxy_config(target, request_timeout_ms=request_timeout_ms)
    return target._backpressure_timeout_s


def holder_seconds(request_timeout_ms: int) -> float:
    return config.lane_response_deadline_s(request_timeout_ms)


class _NoOperatorValue(unittest.TestCase):
    """Every test here is about the DEFAULT, so the operator variables must be absent."""

    def setUp(self) -> None:
        self._saved = {name: os.environ.pop(name, None) for name in BACKPRESSURE_VARS}

    def tearDown(self) -> None:
        for name, value in self._saved.items():
            os.environ.pop(name, None)
            if value is not None:
                os.environ[name] = value


class AWaiterOutlastsTheCallAheadOfItTest(_NoOperatorValue):

    def test_at_every_timeout_the_waiter_can_outlast_one_holder(self) -> None:
        for request_timeout_ms in TIMEOUTS_MS:
            with self.subTest(request_timeout_ms=request_timeout_ms):
                waiter = waiter_seconds(request_timeout_ms)
                holder = holder_seconds(request_timeout_ms)
                self.assertGreaterEqual(
                    waiter, holder,
                    "a caller queued behind one call gives up %.3fs before that call's own "
                    "deadline of %.3fs, so it can never be granted the lane -- every call that "
                    "runs long rejects its whole queue and reports it as backpressure"
                    % (holder - waiter, holder))

    def test_the_reader_uses_that_same_definition(self) -> None:
        """The client must not spell the deadline out again -- that is how they drifted."""
        path = os.path.join(TOOLS, "matrixark_mcp_rust_proxy_client.py")
        with open(path, encoding="utf-8") as handle:
            source = handle.read()
        self.assertIn("lane_response_deadline_s(self.request_timeout_ms)", source,
                      "the lane reader no longer derives its deadline from the shared helper")
        self.assertNotIn("max(2.0, self.request_timeout_ms / 1000.0 + 2.0)", source,
                         "the reader has its own copy of the deadline again")


class AnOperatorValueStillWinsTest(_NoOperatorValue):

    def test_either_spelling_is_honoured(self) -> None:
        for name in BACKPRESSURE_VARS:
            with self.subTest(variable=name):
                os.environ[name] = "1500"
                try:
                    self.assertAlmostEqual(1.5, waiter_seconds(30000), places=6)
                finally:
                    os.environ.pop(name, None)

    def test_the_proxy_spelling_takes_precedence(self) -> None:
        os.environ["MATRIXARK_RUST_PROXY_BACKPRESSURE_TIMEOUT_MS"] = "1500"
        os.environ["MATRIXARK_RUST_GATEWAY_BACKPRESSURE_TIMEOUT_MS"] = "9000"
        try:
            self.assertAlmostEqual(1.5, waiter_seconds(30000), places=6)
        finally:
            for name in BACKPRESSURE_VARS:
                os.environ.pop(name, None)

    def test_a_blank_value_is_not_a_value(self) -> None:
        """`export ...=` reaches the reader as "", and int("") raises at construction."""
        for name in BACKPRESSURE_VARS:
            with self.subTest(variable=name):
                os.environ[name] = ""
                try:
                    self.assertAlmostEqual(holder_seconds(30000), waiter_seconds(30000), places=6)
                finally:
                    os.environ.pop(name, None)

    def test_both_blank_still_falls_through_to_the_default(self) -> None:
        for name in BACKPRESSURE_VARS:
            os.environ[name] = "   "
        try:
            self.assertAlmostEqual(holder_seconds(30000), waiter_seconds(30000), places=6)
        finally:
            for name in BACKPRESSURE_VARS:
                os.environ.pop(name, None)


class TheCheckCanFailTest(_NoOperatorValue):
    """The floor. The comparison above passes trivially if the two numbers are read from one
    place by accident, or if the helper stops depending on its argument at all."""

    def test_the_comparison_rejects_a_waiter_that_gives_up_early(self) -> None:
        """The defect, reconstructed: a waiter derived from the request timeout alone."""
        for request_timeout_ms in (5000, 30000, 60000):
            with self.subTest(request_timeout_ms=request_timeout_ms):
                broken_waiter = request_timeout_ms / 1000.0
                self.assertLess(broken_waiter, holder_seconds(request_timeout_ms),
                                "the old derivation is no longer detectably short, so this "
                                "check would not have caught the defect it was written for")

    def test_the_deadline_actually_grows_with_the_timeout(self) -> None:
        """A helper returning a constant satisfies every assertion above."""
        seen = [holder_seconds(ms) for ms in TIMEOUTS_MS]
        self.assertGreater(len(set(seen)), 1, "the deadline does not depend on its argument")
        self.assertEqual(seen, sorted(seen), "a longer timeout must not shorten the deadline")

    def test_the_waiter_also_grows_with_the_timeout(self) -> None:
        seen = [waiter_seconds(ms) for ms in TIMEOUTS_MS]
        self.assertGreater(len(set(seen)), 1, "the backpressure timeout ignores its argument")


if __name__ == "__main__":
    unittest.main()
