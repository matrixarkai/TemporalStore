#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A caller queued on a proxy lane must be willing to wait longer than the call ahead of it.

Two timeouts govern one lane. A call holding the lane blocks in `_read_json_line` until its own
deadline; a caller queued behind it blocks in `semaphore.acquire(timeout=_backpressure_timeout_s)`.
They were derived from different numbers:

    holder   max(2.0, request_timeout_ms / 1000 + 2.0)      <- request timeout PLUS two seconds
    waiter   request_timeout_ms / 1000                      <- request timeout

The waiter gave up exactly 2.0 s before the holder's own deadline, at every setting. So a call that
ran to its deadline was **guaranteed** to reject every caller queued behind it -- not under load, by
construction. The error says

    rejected by pack proxy lane backpressure after 40.000s with 4 workers

which reads as a saturated lane and sends the reader to worker pools; the real condition is one slow
call whose queue was never admissible.

**TWO modules serve this lane**, and the first fix reached only one of them -- the unwired
pre-split client in `matrixark_mcp_rust_proxy_client`, while the live client that
`matrixark_mcp_server` imports lives in `matrixark_mcp_temporal_adapters` and still gave its
waiters the smaller number. That is the second time a fix on this lane reached one of two copies:
`matrixark_json_lane` exists because the faster JSON decoder did exactly the same thing.

So the deadline now lives in `matrixark_json_lane` beside `lane_loads`, for the same reason, and
this file checks **every** module that serves the lane rather than a list of the ones I happened to
look at. The set is derived -- a module defining `_read_json_line` serves the lane -- so a third
copy is covered the day it appears.
"""
from __future__ import annotations

import ast
import glob
import io
import os
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
if TOOLS not in os.sys.path:
    os.sys.path.insert(0, TOOLS)

from matrixark_json_lane import LANE_RESPONSE_GRACE_S, lane_response_deadline_s  # noqa: E402

#: Two today. A floor, not a count: a third lane server must be covered, not silently skipped.
LANE_SERVER_FLOOR = 2

#: The formula, written out. Its presence in a lane server means that module has its own copy.
INLINE_FORMULA = "request_timeout_ms / 1000.0 + 2.0"

TIMEOUTS_MS = (0, 1, 250, 2000, 5000, 10000, 30000, 38000, 60000, 120000)

BACKPRESSURE_VARS = (
    "MATRIXARK_RUST_PROXY_BACKPRESSURE_TIMEOUT_MS",
    "MATRIXARK_RUST_GATEWAY_BACKPRESSURE_TIMEOUT_MS",
)


def source_of(stem: str) -> str:
    with io.open(os.path.join(TOOLS, stem + ".py"), encoding="utf-8") as handle:
        return handle.read()


def lane_servers() -> list:
    """Every module defining `_read_json_line` -- found, not listed."""
    out = []
    for path in sorted(glob.glob(os.path.join(TOOLS, "*.py"))):
        stem = os.path.basename(path)[:-3]
        if stem.startswith("test_"):
            continue
        try:
            with io.open(path, encoding="utf-8") as handle:
                tree = ast.parse(handle.read())
        except (OSError, SyntaxError):
            continue
        for node in ast.walk(tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) \
                    and node.name == "_read_json_line":
                out.append(stem)
                break
    return out


class EveryLaneServerUsesTheSharedDeadlineTest(unittest.TestCase):

    def test_the_lane_servers_are_found(self) -> None:
        """The floor. With an empty set every assertion below passes over nothing."""
        servers = lane_servers()
        self.assertGreaterEqual(
            len(servers), LANE_SERVER_FLOOR,
            "found %d modules defining _read_json_line, expected at least %d -- this check has "
            "gone blind, or the lane moved and it needs re-aiming" % (len(servers), LANE_SERVER_FLOOR))

    def test_no_lane_server_spells_the_deadline_itself(self) -> None:
        """A module with its own copy of the formula is a module the next fix will miss."""
        for stem in lane_servers():
            with self.subTest(module=stem):
                # assertTrue, not assertNotIn: the haystack is a 280 KB module and unittest
                # prints the whole thing, which buries the one line that matters.
                self.assertTrue(
                    INLINE_FORMULA not in source_of(stem),
                    "%s spells the lane deadline out itself (%r). Both copies drifted apart "
                    "last time; derive it from matrixark_json_lane instead."
                    % (stem, INLINE_FORMULA))

    def test_every_lane_server_derives_it_from_the_shared_helper(self) -> None:
        for stem in lane_servers():
            with self.subTest(module=stem):
                self.assertTrue("lane_response_deadline_s" in source_of(stem),
                                "%s does not use the shared lane deadline" % stem)

    def test_the_live_client_is_covered(self) -> None:
        """Named on purpose. The first fix reached only the UNWIRED copy, and a derived set that
        happened to miss the live one would have looked just as green."""
        self.assertIn("matrixark_mcp_temporal_adapters", lane_servers(),
                      "the live client that matrixark_mcp_server imports is not being checked")


class AWaiterOutlastsTheCallAheadOfItTest(unittest.TestCase):

    def setUp(self) -> None:
        self._saved = {name: os.environ.pop(name, None) for name in BACKPRESSURE_VARS}

    def tearDown(self) -> None:
        for name, value in self._saved.items():
            os.environ.pop(name, None)
            if value is not None:
                os.environ[name] = value

    def test_the_waiter_default_is_the_holder_deadline(self) -> None:
        """Both sides come from one number, so the waiter cannot be the smaller of the two."""
        import matrixark_mcp_rust_proxy_config as config

        class _Target:
            pass

        for request_timeout_ms in TIMEOUTS_MS:
            with self.subTest(request_timeout_ms=request_timeout_ms):
                target = _Target()
                config.initialize_rust_proxy_config(target, request_timeout_ms=request_timeout_ms)
                holder = lane_response_deadline_s(request_timeout_ms)
                self.assertGreaterEqual(
                    target._backpressure_timeout_s, holder,
                    "a caller queued behind one call gives up %.3fs before that call's own "
                    "deadline of %.3fs, so it can never be granted the lane"
                    % (holder - target._backpressure_timeout_s, holder))

    def test_the_live_client_takes_the_same_default(self) -> None:
        """Source-level, because constructing the live client starts a proxy process."""
        source = source_of("matrixark_mcp_temporal_adapters")
        self.assertTrue("_LANE_DEADLINE_S(request_timeout_ms)" in source,
                      "the live client's backpressure default no longer comes from the lane "
                      "deadline, which is exactly how the waiter came to be the smaller number")


class TheCheckCanFailTest(unittest.TestCase):

    def test_the_old_derivation_is_still_detectably_short(self) -> None:
        for request_timeout_ms in (5000, 30000, 60000):
            with self.subTest(request_timeout_ms=request_timeout_ms):
                self.assertLess(request_timeout_ms / 1000.0,
                                lane_response_deadline_s(request_timeout_ms),
                                "the old derivation no longer reads short, so this check would "
                                "not have caught the defect it was written for")

    def test_the_deadline_depends_on_its_argument(self) -> None:
        seen = [lane_response_deadline_s(ms) for ms in TIMEOUTS_MS]
        self.assertGreater(len(set(seen)), 1, "the deadline ignores its argument")
        self.assertEqual(seen, sorted(seen), "a longer timeout must not shorten the deadline")

    def test_the_grace_is_a_positive_number(self) -> None:
        self.assertGreater(LANE_RESPONSE_GRACE_S, 0)


if __name__ == "__main__":
    unittest.main()
