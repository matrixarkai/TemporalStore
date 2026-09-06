#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Two readers of one variable should agree about its STRING default.

`test_flag_readers_agree` asks this of booleans and `test_numeric_defaults_agree` of numbers. A
string was covered by neither, and drifts exactly the same way: two readers agree on every value
anyone sets and part company only for the deployment that leaves the variable alone -- which is most
of them, and the one nobody tests.

The worked example is why this file exists. `MATRIXARK_NAMESPACE` and `MATRIXARK_TABLE` resolved to
"matrixark" and "context" in two tools while `config/temporalstore.toml` declared `deploy_ns` and
`deploy_table` for those exact names, and every running process carried the second pair. Nothing in
the repository named the first pair at all. A backfill run without `--namespace` therefore addressed
a store the deployment does not read -- and reported having done so, because writing into an empty
namespace succeeds. Struck in mx#1056.

Most differences here are not that. A variable that names a PROVIDER or an AGENT legitimately
resolves differently per reader: an API base is per provider, and the two agent hooks are two
tenants on purpose. Each is listed below with what makes it legitimate, so the next one has to be
looked at rather than joining them quietly.

The set is asserted exactly: a NEW disagreement fails here, and a listed variable that stops
disagreeing fails too, because a list allowed to rot describes a tree that no longer exists.
"""
from __future__ import annotations

import collections
import os
import re
import subprocess
import unittest
from typing import Dict, List, Tuple

TOOLS = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(TOOLS)

_READ = re.compile(
    r'os\.(?:environ\.get|getenv)\(\s*["\']((?:TS|MATRIXARK|TEMPORALSTORE)_[A-Z0-9_]+)["\']\s*,\s*'
    r'["\']([^"\']*)["\']\s*\)')

#: Values that are really booleans or numbers wearing quotes. Their own guards own them.
_NOT_A_STRING = {"", "0", "1", "true", "false", "yes", "no", "on", "off",
                 "TRUE", "FALSE", "YES", "NO", "ON", "OFF", "True", "False"}

#: Variables whose readers legitimately choose different strings, and what makes it legitimate.
KNOWN_DISAGREEMENTS: Dict[str, str] = {
    "MATRIXARK_ACCESS_MODE":
        "the entry points are not peers on this: see test_the_two_asgi_fronts_disagree_on_purpose, "
        "which declares each front's default and the reason",
    "MATRIXARK_ACCOUNT_ID":
        "acct_agent and acct_codex -- the two agent hooks are two accounts, which is the point",
    "MATRIXARK_TENANT_ID":
        "tenant_agent and tenant_codex, the same separation one level down",
    "MATRIXARK_TEAM":
        "agent and codex, naming which hook is writing",
    "MATRIXARK_TEMPORALSTORE_PREFIX":
        "matrixark:agent-hook, matrixark:codex-hook and matrixark:mcp -- a key prefix that exists "
        "to keep the three writers apart",
    "MATRIXARK_EMBEDDING_API_BASE":
        "the OpenAI and Voyage endpoints, chosen in one function by which provider is configured",
    "MATRIXARK_EMBEDDING_API_KEY_ENV":
        "OPENAI_API_KEY and VOYAGE_API_KEY, picked beside the base above",
    "MATRIXARK_EXTRACTION_API_KEY_ENV":
        "OPENAI_API_KEY and ANTHROPIC_API_KEY, the same shape for the extraction provider",
    "MATRIXARK_HTTP_HOST":
        "0.0.0.0 for the gateway, which serves a network, and 127.0.0.1 for the two MCP entry "
        "points, whose HTTP mode is a local portal facade",
    "MATRIXARK_METADATA_BACKEND":
        "record_log is what a deployment gets; the SQL probe reads mysql to decide whether to "
        "demand a DSN before asserting the store it built is the SQL one",
    "MATRIXARK_TEMPORALSTORE_NAMESPACE":
        "deploy_ns everywhere except the scale-failover harness, which stands up its own store "
        "and addresses it by its own name",
    "MATRIXARK_TEMPORALSTORE_TABLE":
        "deploy_table, with the same harness exception",
}

#: 70 variables carried a string default when this was written.
EXPECTED_STRING_READ_FLOOR = 40


def _production_sources() -> List[str]:
    listed = subprocess.run(["git", "ls-files", "*.py"], cwd=REPO,
                            capture_output=True, text=True).stdout.split()
    return [path for path in listed if not os.path.basename(path).startswith("test_")]


def _reads() -> Dict[str, List[Tuple[str, int, str]]]:
    found: Dict[str, List[Tuple[str, int, str]]] = collections.defaultdict(list)
    for path in _production_sources():
        try:
            with open(os.path.join(REPO, path), encoding="utf-8", errors="replace") as handle:
                lines = handle.read().splitlines()
        except OSError:
            continue
        for number, line in enumerate(lines, 1):
            for match in _READ.finditer(line):
                value = match.group(2)
                if value in _NOT_A_STRING or re.fullmatch(r"-?\d+(\.\d+)?", value):
                    continue
                found[match.group(1)].append((path, number, value))
    return found


def _disagreeing() -> set:
    return {name for name, rows in _reads().items() if len({v for _, _, v in rows}) > 1}


class StringDefaultsAgreeTest(unittest.TestCase):

    def test_the_scan_still_finds_string_defaults(self) -> None:
        reads = _reads()
        self.assertGreaterEqual(
            len(reads), EXPECTED_STRING_READ_FLOOR,
            "found %d variables read with a string default, expected at least %d -- if the read "
            "shape changed, the assertions below run on an empty set"
            % (len(reads), EXPECTED_STRING_READ_FLOOR))

    def test_no_new_variable_disagrees_about_its_default(self) -> None:
        reads = _reads()
        new = sorted(_disagreeing() - set(KNOWN_DISAGREEMENTS))
        detail = ["%s (%s)" % (name, ", ".join(sorted({v for _, _, v in reads[name]})))
                  for name in new]
        self.assertEqual(
            [], detail,
            "these are read with more than one string default, so a deployment that sets nothing "
            "gets a different value depending on which path asks: %s\nA name, a namespace or an "
            "endpoint that differs by reader is how a tool ends up addressing something nobody "
            "else does. Make them agree, or list it above with what makes it legitimate." % detail)

    def test_a_listed_variable_that_now_agrees_is_struck_off(self) -> None:
        stale = sorted(set(KNOWN_DISAGREEMENTS) - _disagreeing())
        self.assertEqual(
            [], stale,
            "these are listed as disagreeing and no longer do: %s. Strike them off." % stale)

    def test_every_listed_variable_gives_a_reason(self) -> None:
        thin = sorted(name for name, why in KNOWN_DISAGREEMENTS.items() if len(why.strip()) < 30)
        self.assertEqual([], thin, "listed without a reason worth reading: %s" % thin)


if __name__ == "__main__":
    unittest.main()
