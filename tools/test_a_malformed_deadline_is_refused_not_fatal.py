#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A malformed argument must be refused, not crash the tool call.

`retrieval_deadline_ms` converts `deadline_ms` inside a try and raises
`MatrixArkError("deadline_ms must be an integer")`. It never saw a bad value:
`dispatch_matrixark_tool` converts the same argument first, and did it unguarded and outside the
try/except that wraps the retrieve, so a client sending a non-numeric deadline_ms got a bare
`ValueError: invalid literal for int()` out of `call_tool`.

The neighbouring argument shows what the difference is worth -- `limit` took the same input and
came back with a pack, because nothing converts it early. One malformed argument was a structured
refusal and the other an unhandled crash, decided by which layer happened to convert first.

Asserted through `server.call_tool`, because that is where a caller stands. A unit test on
`retrieval_deadline_ms` passes with the defect present: that function was always correct, and was
never reached.
"""
from __future__ import annotations

import inspect
import tempfile
import unittest
from pathlib import Path

import matrixark_mcp_server as mcp

SCOPE = {"account_id": "acct_local", "tenant_id": "deadline", "user_id": "u",
         "session_id": "s0", "agent_name": "t"}


def _dispatch_error_class():
    """The MatrixArkError class the DISPATCH module holds.

    `matrixark_mcp_errors` and `tools.matrixark_mcp_errors` are two module objects carrying two
    distinct classes -- neither a subclass of the other -- so `assertRaises` against the wrong one
    does not catch, and the failure reads as "no error was raised" when one was. Taken from the
    module under test rather than by importing a spelling.
    """
    import matrixark_mcp_dispatch

    module = inspect.getmodule(matrixark_mcp_dispatch.dispatch_matrixark_tool)
    return module.MatrixArkError


def _server():
    adapter = mcp.MatrixArkLocalAdapter(Path(tempfile.mkdtemp()) / "d.jsonl")
    return mcp.MatrixArkMcpServer(adapter, access_mode="dev")


class AMalformedDeadlineIsRefusedNotFatalTest(unittest.TestCase):

    def test_a_non_numeric_deadline_is_refused(self) -> None:
        """The defect, at the layer a caller reaches."""
        with self.assertRaises(_dispatch_error_class()) as caught:
            _server().call_tool("matrixark_retrieve",
                                {"scope": SCOPE, "query": "x", "deadline_ms": "not-an-int"})
        self.assertIn("deadline_ms", str(caught.exception))

    def test_the_refusal_is_the_structured_error_not_a_bare_value_error(self) -> None:
        """MatrixArkError subclasses ValueError, so asserting ValueError would have passed against
        the crash this test exists to stop. The class has to be the specific one."""
        try:
            _server().call_tool("matrixark_retrieve",
                                {"scope": SCOPE, "query": "x", "deadline_ms": "not-an-int"})
        except BaseException as exc:                     # noqa: BLE001 - the type is the assertion
            self.assertIsNot(
                type(exc), ValueError,
                "the tool call failed with a bare ValueError, so a malformed argument is a crash "
                "rather than a refusal a caller can act on")
            self.assertIsInstance(exc, _dispatch_error_class())
        else:
            self.fail("a non-numeric deadline_ms was accepted")

    def test_a_well_formed_deadline_still_works(self) -> None:
        """The guard must reject the malformed value and nothing else. A conversion wrapped so
        broadly that it swallows good input would pass the two assertions above."""
        out = _server().call_tool("matrixark_retrieve",
                                  {"scope": SCOPE, "query": "x", "deadline_ms": 5000})
        self.assertIsInstance(out, dict)
        self.assertIn("context_pack_id", out)

    def test_an_absent_deadline_still_works(self) -> None:
        """The common case: most callers send none at all, and the conversion has an `or 0` path
        that must stay reachable."""
        out = _server().call_tool("matrixark_retrieve", {"scope": SCOPE, "query": "x"})
        self.assertIsInstance(out, dict)
        self.assertIn("context_pack_id", out)

    def test_the_error_class_asserted_on_is_the_one_the_dispatch_raises(self) -> None:
        """A floor. Two spellings of `matrixark_mcp_errors` are loaded and carry two distinct
        classes, neither a subclass of the other. Asserting against the wrong one would report
        that no error was raised while one was."""
        import matrixark_mcp_dispatch

        module = inspect.getmodule(matrixark_mcp_dispatch.dispatch_matrixark_tool)
        self.assertIs(
            _dispatch_error_class(), module.MatrixArkError,
            "this file asserts on a different MatrixArkError than the dispatch raises")
        self.assertTrue(
            issubclass(_dispatch_error_class(), ValueError),
            "MatrixArkError no longer subclasses ValueError, so the bare-ValueError assertion "
            "above is checking something else than it reads")


if __name__ == "__main__":
    unittest.main()
