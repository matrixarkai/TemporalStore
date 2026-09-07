#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A shell default that contradicts the shipped config has to say so.

`test_config_says_when_it_overrides` asks this of the engine and
`test_config_says_when_it_overrides_a_python_reader` asks it of Python. Neither reads SHELL, and a
wrapper script is what an operator actually runs:

    NAMESPACE="${NAMESPACE:-${MATRIXARK_NAMESPACE:-matrixark}}"

That line defaulted a backfill run to the namespace `matrixark` while the shipped config, every
running process and both Python readers said `deploy_ns`. So the run addressed a store the
deployment does not read and reported success, because writing into an empty namespace succeeds.
mx#1056 struck that exact pair out of two PYTHON tools; the guard keeping it struck scans `.py`,
so the wrapper kept it -- and the line directly above it in the same file already carried the
correction for the metaserver, for the same reason.

The set is asserted exactly. A new contradiction fails here, and a listed one that stops
contradicting fails too, because a list allowed to rot describes a tree that no longer exists.
"""
from __future__ import annotations

import os
import re
import subprocess
import unittest
from typing import Dict, Set

TOOLS = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(TOOLS)
CONFIG = os.path.join(REPO, "config", "temporalstore.toml")

#: Shell defaults that deliberately differ from the shipped config, and why. A deployment script
#: choosing its own shape is not the same as a wrapper quietly addressing the wrong store.
KNOWN_OVERRIDES: Dict[str, str] = {
    "MATRIXARK_EMBED_DRAINER":
        "the deploy profile turns the drainer on, annotated beside the line -- drain dirty rows "
        "twice a second so a write is searchable without waiting for a batch job -- while the "
        "engine and the config leave it off",
    "TS_CACHE_MEMORY_BYTES":
        "the docker entrypoint and the one-box deploy size their own cache, in gigabytes, "
        "against a config value meant for a small local run",
    "TS_SERVER_NODE_ID":
        "deploy_onebox pins node 1; the config ships 0, which means auto",
    "TS_STORAGE_BACKEND":
        "deploy_onebox and deploy_raft each deploy a named backend; the config ships auto",
}

_SECTION = re.compile(r"^\s*\[([a-z0-9_.]+)\]")
_CONFIG_LINE = re.compile(
    r"^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.+?)\s+#\s*"
    r"((?:TS|MATRIXARK|TEMPORALSTORE)_[A-Z0-9_]+)")
_SHELL_DEFAULT = re.compile(
    r"\$\{((?:TS|MATRIXARK|TEMPORALSTORE)_[A-Z0-9_]+):-([^}$]*)\}")

#: Floored so a scan that stops matching fails instead of passing on an empty set.
EXPECTED_COMPARABLE_FLOOR = 6

_QUOTES = chr(34) + chr(39)
_TRUE = {"1", "true", "yes", "on"}
_FALSE = {"0", "false", "no", "off"}


def _normalise(value: str) -> str:
    text = str(value).strip().strip(_QUOTES).lower()
    if text in _TRUE:
        return "on"
    if text in _FALSE:
        return "off"
    return text


def _config_values() -> Dict[str, str]:
    section = ""
    values: Dict[str, str] = {}
    with open(CONFIG, encoding="utf-8") as handle:
        for raw in handle:
            found = _SECTION.match(raw)
            if found:
                section = found.group(1)
                continue
            match = _CONFIG_LINE.match(raw)
            if match:
                values[match.group(3)] = _normalise(match.group(2))
    return values


def _shell_defaults() -> Dict[str, Set[str]]:
    listed = subprocess.run(["git", "ls-files", "*.sh"], cwd=REPO,
                            capture_output=True, text=True).stdout.split()
    found: Dict[str, Set[str]] = {}
    for relative in listed:
        try:
            with open(os.path.join(REPO, relative), encoding="utf-8", errors="replace") as handle:
                text = handle.read()
        except OSError:
            continue
        for match in _SHELL_DEFAULT.finditer(text):
            found.setdefault(match.group(1), set()).add(_normalise(match.group(2)))
    return found


def _disagreeing() -> Set[str]:
    config, shell = _config_values(), _shell_defaults()
    return {name for name, values in shell.items()
            if name in config and config[name] not in values}


class AShellDefaultAgreesWithTheShippedConfigTest(unittest.TestCase):

    def test_the_scan_still_compares_something(self) -> None:
        config, shell = _config_values(), _shell_defaults()
        comparable = set(config) & set(shell)
        self.assertGreaterEqual(
            len(comparable), EXPECTED_COMPARABLE_FLOOR,
            "only %d variables have both a shell default and a config value, expected at least "
            "%d -- if either shape changed, the assertions below run on an empty set"
            % (len(comparable), EXPECTED_COMPARABLE_FLOOR))

    def test_no_shell_default_contradicts_the_config_without_saying_so(self) -> None:
        new = sorted(_disagreeing() - set(KNOWN_OVERRIDES))
        self.assertEqual(
            [], new,
            "these shell defaults send a script somewhere the shipped config does not: %s. A "
            "wrapper that defaults to a namespace nobody serves runs, succeeds and writes "
            "nowhere anyone reads." % new)

    def test_a_listed_override_that_now_agrees_is_struck_off(self) -> None:
        stale = sorted(set(KNOWN_OVERRIDES) - _disagreeing())
        self.assertEqual(
            [], stale, "these are listed as overriding and no longer do: %s. Strike them off."
            % stale)

    def test_every_listed_override_gives_a_reason(self) -> None:
        thin = sorted(name for name, why in KNOWN_OVERRIDES.items() if len(why.strip()) < 30)
        self.assertEqual([], thin, "listed without a reason worth reading: %s" % thin)


if __name__ == "__main__":
    unittest.main()
