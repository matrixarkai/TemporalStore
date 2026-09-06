#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Two readers of one variable should agree about its numeric default.

`test_flag_readers_agree` asks this of booleans, where a disagreement shows up as a setting that
half-applies. A NUMBER drifts the same way and shows less: two readers with different fallbacks
agree on every value anyone sets, and part company only for the deployment that leaves the variable
alone -- which is most of them, and the one nobody tests.

`MATRIXARK_DEFAULT_MAX_CONTEXT_TOKENS` was the worked example, and is now struck off. The backend
resolved an omitted budget to 500000 while the two agent hooks passed 128000, so an operator who set
nothing got a quarter of the documented window through one path and all of it through another. The
schema used to quote the wrong one of those two numbers at callers, which is what
`test_matrixark_the_schema_quotes_the_budget_it_applies` was written for -- the same pair of numbers,
one layer up. Both readers now agree, so the entry goes: the rule below is that a list of known
differences which is allowed to go stale is read as a description of the tree.

The remaining three are NOT endorsed here. One looks like a deliberate difference between a client
and the server it calls, and one is two reader paths in a benchmark. Neither has been examined, and
this file does not pretend otherwise: it exists so the next one has to be looked at rather than
joining them quietly. Strike an entry when its difference is either justified in the code or
removed.

The two TemporalStore SDK timeouts are struck. They were not a client/server difference: one option
with one meaning, declared by three parsers, defaulting to 20000 in the backend resolver and 60000
in both agent hooks. Every launcher supplies 60000 -- the three installers, the codex hook wrapper,
the topology waiter, and `matrixark_mcp_rust_server.sh`, which starts the very server the 20000
belonged to and passes the value explicitly. So the short number was reached only by a server
started outside every shipped path, and the resolver now agrees at 60000.
"""
from __future__ import annotations

import collections
import os
import re
import subprocess
import unittest
from typing import Dict, List, Set, Tuple

TOOLS = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(TOOLS)

_READ = re.compile(
    r'os\.(?:environ\.get|getenv)\(\s*["\']((?:TS|MATRIXARK|TEMPORALSTORE)_[A-Z0-9_]+)["\']\s*,\s*'
    r'["\']?(-?\d+)["\']?\s*\)')

# Known, unexamined. Each is a variable whose readers do not agree on the number to use when it is
# unset. The note says what the difference looks like, not that it is right.
KNOWN_DISAGREEMENTS: Dict[str, str] = {
    "MATRIXARK_HTTP_PORT":
        "servers bind 8080, the CLI paths use 0 (an ephemeral port)",
    "MATRIXARK_READER_MAX_TOKENS":
        "two reader paths in one benchmark script, 160 and 64",
    "MATRIXARK_RETRIEVAL_TIMEOUT_MS":
        "the retrieve paths use 0 (no deadline), the MCP server 30000",
}

# 196 when this was written.
EXPECTED_NUMERIC_READ_FLOOR = 120


def _production_sources() -> List[str]:
    listed = subprocess.run(["git", "ls-files", "*.py"], cwd=REPO,
                            capture_output=True, text=True).stdout.split()
    return [path for path in listed if not os.path.basename(path).startswith("test_")]


def _numeric_reads() -> Dict[str, List[Tuple[str, int, str]]]:
    found: Dict[str, List[Tuple[str, int, str]]] = collections.defaultdict(list)
    for path in _production_sources():
        try:
            with open(os.path.join(REPO, path), encoding="utf-8", errors="replace") as handle:
                lines = handle.read().splitlines()
        except OSError:
            continue
        for number, line in enumerate(lines, 1):
            for match in _READ.finditer(line):
                found[match.group(1)].append((path, number, match.group(2)))
    return found


def _disagreeing() -> Set[str]:
    return {name for name, entries in _numeric_reads().items()
            if len({value for _, _, value in entries}) > 1}


class NumericDefaultsAgreeTest(unittest.TestCase):

    def test_the_scan_still_finds_numeric_reads(self) -> None:
        reads = _numeric_reads()
        self.assertGreaterEqual(
            len(reads), EXPECTED_NUMERIC_READ_FLOOR,
            "found %d variables read with a numeric default, expected at least %d -- if the read "
            "shape changed, the assertions below pass on an empty set"
            % (len(reads), EXPECTED_NUMERIC_READ_FLOOR))

    def test_the_list_has_not_emptied(self) -> None:
        self.assertTrue(
            KNOWN_DISAGREEMENTS,
            "the list is empty, so the check below cannot tell a clean tree from a broken scan.")

    def test_no_new_variable_disagrees_about_its_default(self) -> None:
        reads = _numeric_reads()
        new = sorted(_disagreeing() - set(KNOWN_DISAGREEMENTS))
        detail = []
        for name in new:
            values = sorted({value for _, _, value in reads[name]})
            detail.append("%s (%s)" % (name, ", ".join(values)))
        self.assertEqual(
            [], detail,
            "these are read with more than one numeric default, so a deployment that sets nothing "
            "gets a different number depending on which path asks: %s\nMake them agree, or list it "
            "with what the difference is." % detail)

    def test_a_listed_variable_that_now_agrees_is_struck_off(self) -> None:
        settled = sorted(set(KNOWN_DISAGREEMENTS) - _disagreeing())
        self.assertEqual(
            [], settled,
            "these are listed as disagreeing and no longer do: %s. Strike them off -- a list of "
            "known differences that is allowed to go stale is read as a description of the tree."
            % settled)


if __name__ == "__main__":
    unittest.main()
