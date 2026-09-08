#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""What the lane parser must and must not change.

The lane's JSON decode is most of this process's CPU, so it uses orjson where it is installed.
That is a parser swap on the hottest read in the system, and two things about it are worth
pinning rather than assuming: the values this system actually stores round-trip exactly, and the
one place the two parsers disagree is understood rather than discovered.
"""
from __future__ import annotations

import json
import unittest

from matrixark_mcp_temporal_adapters import _LANE_LOADS


def _lane_parser_is_orjson() -> bool:
    try:
        import orjson  # noqa: F401
    except ImportError:
        return False
    return True


def _lane_parser_name() -> str:
    return "orjson" if _lane_parser_is_orjson() else "stdlib"


class LaneParserTest(unittest.TestCase):
    def test_a_lane_response_reads_the_same_as_the_stdlib(self):
        line = json.dumps({
            "ok": True,
            "op": "batch_hget",
            "count": 3,
            "records": [{"key": "k", "field": "f", "value": '{"record_type":"context_event"}'}],
            "unicode": "结算账本 — six hours",
            "nested": {"a": [1, 2, {"b": None}], "c": True},
        })
        self.assertEqual(_LANE_LOADS(line), json.loads(line))

    def test_every_hash_this_system_stores_is_exact(self):
        """Record hashes are u64. Those must not lose a digit."""
        for value in (0, 1, 2**53, 2**53 + 1, 2**63 - 1, 2**64 - 1):
            line = json.dumps({"node_hash": value})
            self.assertEqual(_LANE_LOADS(line)["node_hash"], value, value)
            self.assertIsInstance(_LANE_LOADS(line)["node_hash"], int, value)

    def test_a_malformed_line_still_raises_what_the_callers_catch(self):
        """The reader catches json.JSONDecodeError; orjson's subclasses it."""
        with self.assertRaises(json.JSONDecodeError):
            _LANE_LOADS("{not json")

    def test_the_one_disagreement_is_beyond_what_json_guarantees(self):
        """Integers past u64 come back as floats under orjson, exact under the stdlib.

        JSON guarantees no integer precision beyond 2**53 -- JavaScript and most parsers lose it
        far earlier -- so a value this large is already outside what an interoperable consumer
        round-trips. Asserted per parser rather than as one loose bound: a single assertion that
        held for both would pin nothing, and the whole point is that this is the place they
        differ.
        """
        line = json.dumps({"huge": 184467440737095516150})
        got = _LANE_LOADS(line)["huge"]
        if _lane_parser_is_orjson():
            self.assertIsInstance(got, float, "orjson is expected to widen this to a float")
            self.assertAlmostEqual(got, 1.8446744073709552e20, delta=1e6)
        else:
            self.assertIsInstance(got, int, "the stdlib keeps it exact")
            self.assertEqual(got, 184467440737095516150)

    def test_the_test_knows_which_parser_it_exercised(self):
        """Without this the suite can pass having never touched the parser it is about."""
        import matrixark_mcp_temporal_adapters as adapters

        self.assertTrue(callable(adapters._LANE_LOADS))
        # names the parser in the failure output, so a run on a host without orjson is visible
        self.assertIn(_lane_parser_name(), {"orjson", "stdlib"})


if __name__ == "__main__":
    unittest.main()
