#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A shipped-config key that a PYTHON reader contradicts has to say so.

`test_config_says_when_it_overrides` asks this of the engine: it compares `config/temporalstore.toml`
against the flag inventory, so it sees the `TS_*` half. The same file names 56 `MATRIXARK_*`
variables, 35 of which only Python reads, and nothing compared those.

The distinction is the one the sibling guard is built on. A key that restates a reader's default
documents it, and deleting the line changes nothing. A key that contradicts one is the deployment's
decision, applied -- `matrixark_load_config` writes resolved keys into `os.environ`, so the config's
number is what a reader that "defaults" actually gets. From the line alone the two are
indistinguishable, and the difference is the whole value of the file to somebody deciding what
their deployment does.

Eleven keys contradict a Python default and every one is deliberate; each is listed below with the
reason, taken from the config's own comment where it gives one. Two were not deliberate and are not
here: `MATRIXARK_NAMESPACE` and `MATRIXARK_TABLE` were defaulted to "matrixark" and "context" by two
tools while the config -- and every running process -- said `deploy_ns` and `deploy_table`. Nothing
else in the repository named either value, so a backfill run without `--namespace` addressed a store
the deployment does not read. They now agree.

The set is asserted exactly: a NEW contradiction fails here rather than arriving quietly, and a
listed key that stops contradicting fails too, because a list allowed to rot describes a tree that
no longer exists.
"""
from __future__ import annotations

import collections
import os
import re
import subprocess
import unittest
from typing import Dict

TOOLS = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(TOOLS)
CONFIG = os.path.join(REPO, "config", "temporalstore.toml")

_CONFIG_LINE = re.compile(
    r'^\s*(?P<key>[a-z_][a-z0-9_]*)\s*=\s*(?P<value>"[^"]*"|[-\d.]+)\s*#\s*'
    r'(?P<var>MATRIXARK_[A-Z0-9_]+)')
_READ = re.compile(
    r'os\.(?:environ\.get|getenv)\(\s*["\'](?P<var>MATRIXARK_[A-Z0-9_]+)["\']\s*,\s*'
    r'(?P<default>"[^"]*"|\'[^\']*\'|[-\d.]+)\s*\)')

#: A shipped-config value that is NOT what the Python reader would have chosen, and why.
KNOWN_OVERRIDES: Dict[str, str] = {
    "MATRIXARK_RETRIEVAL_MIN_SCORE":
        "0.05 against the reader's 0.20 -- the floor that keeps junk out of a pack as the caps "
        "above it rise, which is the point of raising them",
    "MATRIXARK_TOP_K_PER_LAYER":
        "24 against 8, the config's own comment: raised for richer default packs",
    "MATRIXARK_MAX_GLOBAL_CANDIDATES":
        "2048 against 512, part of the same deliberate widening of the retrieval surface",
    "MATRIXARK_MAX_SELECTED_REFS":
        "1000 against 64, the hard cap on refs in a pack, raised with the caps that feed it",
    "MATRIXARK_CROSS_SESSION_MAX_CANDIDATES":
        "200 against 24, the config's own comment: was the main breadth limiter",
    "MATRIXARK_CROSS_SESSION_PROFILE_MAX_CANDIDATES":
        "200 against 48, the config's own comment: raised for consistency with the lane above",
    "MATRIXARK_TIME_COMPRESSION_MAX_RAW_EVENTS_PER_NODE":
        "1024 against 256, raw-event retention per node, raised with the retrieval surface",
    "MATRIXARK_HTTP_HOST":
        "0.0.0.0 against the two MCP entry points' 127.0.0.1. The gateway serves a network and "
        "the MCP portal is a local facade; the config describes the gateway",
    "MATRIXARK_HTTP_PORT":
        "8080 against the two MCP entry points' 0, which is not a port but a mode meaning 'stay "
        "on stdio' -- so this is a bind port against a switch, not two ports",
    "MATRIXARK_EMBED_DRAINER":
        "0 against the gateway's empty string; both are off, and the config states the value the "
        "empty string means",
    "MATRIXARK_TEMPORALSTORE_METASERVER":
        "127.0.0.1:18000 against the empty default in the scale-failover harness, which builds "
        "its own store and is told where it is",
}

#: 56 config keys named a MATRIXARK_ variable when this was written, 35 read by Python.
EXPECTED_COMPARABLE_FLOOR = 25


def _unquote(text: str) -> str:
    text = text.strip()
    if len(text) >= 2 and text[0] in "\"'" and text[-1] == text[0]:
        return text[1:-1]
    return text


def _declared() -> Dict[str, str]:
    found: Dict[str, str] = {}
    with open(CONFIG, encoding="utf-8") as handle:
        for line in handle:
            if line.lstrip().startswith("#"):
                continue
            match = _CONFIG_LINE.match(line)
            if match:
                found[match.group("var")] = _unquote(match.group("value"))
    return found


def _readers() -> Dict[str, list]:
    listed = subprocess.run(["git", "ls-files", "tools/*.py"], cwd=REPO,
                            capture_output=True, text=True).stdout.split()
    found = collections.defaultdict(list)
    for path in listed:
        if os.path.basename(path).startswith("test_"):
            continue
        try:
            with open(os.path.join(REPO, path), encoding="utf-8", errors="replace") as handle:
                lines = handle.read().splitlines()
        except OSError:
            continue
        for number, line in enumerate(lines, 1):
            for match in _READ.finditer(line):
                found[match.group("var")].append(
                    (path, number, _unquote(match.group("default"))))
    return found


def _contradicting() -> Dict[str, list]:
    declared, readers = _declared(), _readers()
    out = {}
    for var, value in declared.items():
        disagree = [row for row in readers.get(var, []) if row[2] != value]
        if disagree:
            out[var] = disagree
    return out


class ConfigSaysWhenItOverridesAPythonReader(unittest.TestCase):

    def test_the_scan_still_pairs_config_keys_with_readers(self) -> None:
        """Assert this file's own extent: if either side stops parsing, everything below passes."""
        declared, readers = _declared(), _readers()
        comparable = [var for var in declared if var in readers]
        self.assertGreaterEqual(
            len(comparable), EXPECTED_COMPARABLE_FLOOR,
            "only %d config keys could be paired with a Python reader, expected at least %d -- if "
            "either shape changed, the assertions below run on an empty set"
            % (len(comparable), EXPECTED_COMPARABLE_FLOOR))

    def test_no_new_config_key_contradicts_a_reader_silently(self) -> None:
        new = sorted(set(_contradicting()) - set(KNOWN_OVERRIDES))
        detail = []
        for var in new:
            rows = _contradicting()[var]
            detail.append("%s (config %r, %s)"
                          % (var, _declared()[var],
                             ", ".join("%s:%d=%r" % row for row in rows)))
        self.assertEqual(
            [], detail,
            "the shipped config sets these to something a Python reader would not have chosen, "
            "and nobody has said why: %s\nThe config is APPLIED -- matrixark_load_config writes "
            "resolved keys into the environment -- so this is what the deployment runs. Make them "
            "agree, or list it above with the reason." % detail)

    def test_a_listed_override_that_now_agrees_is_struck_off(self) -> None:
        stale = sorted(set(KNOWN_OVERRIDES) - set(_contradicting()))
        self.assertEqual(
            [], stale,
            "these are listed as contradicting a reader and no longer do: %s. Strike them off."
            % stale)

    def test_every_override_gives_a_reason(self) -> None:
        thin = sorted(var for var, why in KNOWN_OVERRIDES.items() if len(why.strip()) < 30)
        self.assertEqual([], thin, "listed without a reason worth reading: %s" % thin)


if __name__ == "__main__":
    unittest.main()
