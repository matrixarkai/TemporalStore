#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A fallback that reads the whole store has to say which scan gave up.

Seven places replace a scoped scan with a read of every record there is. Six of them said nothing,
so a scan path that stopped working looked exactly like one that was never taken -- and the cost,
which lands on every turn it happens, showed up only as a store that had got slower.

The seventh did say something, and named the wrong method: it sat in `records_for_get_all` and
reported itself as `prior_context_records`, which is a real method elsewhere in the same class. A
label that points at the wrong function is worse than no label, because it is acted on.

So the interesting assertion is not that the calls exist. It is that each one names the method it
is actually inside -- the only part a copied line gets wrong, and the part no reader checks.
"""
from __future__ import annotations

import os
import re
import tempfile
import unittest
import unittest.mock

TOOLS = os.path.dirname(os.path.abspath(__file__))
MODULE = os.path.join(TOOLS, "matrixark_mcp_temporal_adapters.py")

# Seven sites when this was written. Asserted so a scan that stops matching fails rather than
# reporting that every call is correctly named.
EXPECTED_CALL_FLOOR = 7

_CALL = re.compile(r'_note_full_read_fallback\(\s*"(\w+)"')
_DEF = re.compile(r"^\s*def (\w+)\(")


def _calls_with_enclosing_method():
    """(label, enclosing def, line number) for every call site."""
    with open(MODULE, encoding="utf-8") as handle:
        lines = handle.read().splitlines()
    found = []
    for number, line in enumerate(lines):
        match = _CALL.search(line)
        if not match:
            continue
        enclosing = ""
        for back in range(number, -1, -1):
            outer = _DEF.match(lines[back])
            if outer and not lines[back].startswith("    def _note_full_read_fallback"):
                enclosing = outer.group(1)
                break
        found.append((match.group(1), enclosing, number + 1))
    return found


class EveryFullReadFallbackNamesItselfTest(unittest.TestCase):

    def test_the_scan_still_finds_the_call_sites(self) -> None:
        calls = _calls_with_enclosing_method()
        self.assertGreaterEqual(
            len(calls), EXPECTED_CALL_FLOOR,
            "found %d calls, expected at least %d -- if they moved or were renamed this file is "
            "looking for something that no longer exists, and the assertion below passes on an "
            "empty list" % (len(calls), EXPECTED_CALL_FLOOR))

    def test_each_call_names_the_method_it_is_in(self) -> None:
        wrong = ["%s:%d says %r but is inside %r" % (os.path.basename(MODULE), line, label, method)
                 for label, method, line in _calls_with_enclosing_method() if label != method]
        self.assertEqual(
            [], wrong,
            "a fallback reports a method other than the one it is in, which sends whoever reads "
            "the log to the wrong function: %s" % wrong)

    def test_no_full_read_fallback_is_left_silent(self) -> None:
        with open(MODULE, encoding="utf-8") as handle:
            lines = handle.read().splitlines()
        silent = []
        for number, line in enumerate(lines):
            if not line.strip().startswith("except Exception:"):
                continue
            following = " ".join(lines[number + 1:number + 3])
            if "return self.read_all()" in following:
                silent.append(number + 1)
        self.assertEqual(
            [], silent,
            "these fall back to reading the whole store without saying why, at lines %s. Bind the "
            "exception and call _note_full_read_fallback with this method's name." % silent)


class TheChannelItselfTest(unittest.TestCase):
    """The helper has to be safe in the place it is called: inside an except, on a hot path."""

    @staticmethod
    def _helper():
        try:
            from tools import matrixark_mcp_temporal_adapters as adapters  # noqa: PLC0415
        except ImportError:  # run from tools/ dir
            import matrixark_mcp_temporal_adapters as adapters  # type: ignore  # noqa: PLC0415
        return adapters._note_full_read_fallback

    def test_it_writes_the_method_and_the_reason(self) -> None:
        note = self._helper()
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, "debug.log")
            with unittest.mock.patch.dict(os.environ, {"MATRIXARK_MCP_DEBUG_LOG": path}):
                note("records_for_delete", ValueError("scan unavailable"))
            with open(path, encoding="utf-8") as handle:
                written = handle.read()
        self.assertIn("records_for_delete", written)
        self.assertIn("ValueError", written)
        self.assertIn("scan unavailable", written)

    def test_it_writes_nothing_when_no_file_is_named(self) -> None:
        note = self._helper()
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, "debug.log")
            environment = dict(os.environ)
            environment.pop("MATRIXARK_MCP_DEBUG_LOG", None)
            with unittest.mock.patch.dict(os.environ, environment, clear=True):
                note("records_for_delete", ValueError("scan unavailable"))
            self.assertFalse(
                os.path.exists(path),
                "the default path must not open a file it was never given")

    def test_an_unwritable_destination_does_not_raise(self) -> None:
        note = self._helper()
        with unittest.mock.patch.dict(
                os.environ, {"MATRIXARK_MCP_DEBUG_LOG": "/nonexistent-dir/debug.log"}):
            note("records_for_delete", ValueError("scan unavailable"))  # must not raise


if __name__ == "__main__":
    unittest.main()
