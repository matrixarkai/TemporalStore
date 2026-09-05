#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A launcher must not export a variable nothing reads.

Retiring a flag is half the job: the engine stops consulting it and every surface that OFFERS it
carries on offering. The shipped config has a guard for that and the portal has one. The launch
scripts did not, and that is the surface where it is least visible -- an export puts the variable
into the environment of every process the script starts, where it sits beside the ones that decide
something and cannot be told apart from them.

Three were found when this was written, and one had been made hours earlier by retiring
`TS_INDEX_BINARY` from the engine without touching `deploy_profile_common.sh`. That is the whole
case for a guard rather than a habit.

The criterion is the SAFE one: a name counts as read if it appears ANYWHERE outside an export of
itself. A tighter test -- looking for `environ.get` and its friends -- reported
`MATRIXARK_RUST_PROXY_DEDICATED_CLIENTS` as dead when it is read inside a call spanning four lines,
with the name on one of its own. A check that recommends deleting things has to be wrong in the
safe direction.
"""
from __future__ import annotations

import collections
import os
import re
import subprocess
import unittest
from typing import Dict, List

TOOLS = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(TOOLS)

_EXPORT = re.compile(
    r"^\s*export\s+((?:TS|MATRIXARK|TEMPORALSTORE)_[A-Z0-9_]+)\s*=", re.M)

# 103 when this was written. Asserted so a scan that stops matching fails rather than reporting
# that every export is consulted.
EXPECTED_EXPORT_FLOOR = 80

_SKIP_SUFFIXES = (".png", ".jpg", ".jpeg", ".gz", ".zip", ".pdf", ".ico")


def _tracked(pattern: str = "") -> List[str]:
    args = ["git", "ls-files"] + ([pattern] if pattern else [])
    return subprocess.run(args, cwd=REPO, capture_output=True, text=True).stdout.split()


def _exported() -> Dict[str, List[str]]:
    found: Dict[str, List[str]] = collections.defaultdict(list)
    for rel in _tracked("*.sh"):
        try:
            with open(os.path.join(REPO, rel), encoding="utf-8") as handle:
                text = handle.read()
        except OSError:
            continue
        for match in _EXPORT.finditer(text):
            found[match.group(1)].append(rel)
    return found


def _mentioned_elsewhere(names) -> set:
    seen = set()
    for rel in _tracked():
        if rel.endswith(_SKIP_SUFFIXES):
            continue
        try:
            with open(os.path.join(REPO, rel), encoding="utf-8", errors="replace") as handle:
                lines = handle.read().split("\n")
        except OSError:
            continue
        is_shell = rel.endswith(".sh")
        for line in lines:
            for name in names:
                if name in seen or name not in line:
                    continue
                if is_shell and re.match(r"\s*export\s+%s\s*=" % re.escape(name), line):
                    continue  # an export of itself is not a reader
                seen.add(name)
    return seen


class NoLauncherExportsADeadVariableTest(unittest.TestCase):

    def test_the_scan_still_finds_the_exports(self) -> None:
        exported = _exported()
        self.assertGreaterEqual(
            len(exported), EXPECTED_EXPORT_FLOOR,
            "found %d exported variables, expected at least %d -- if the scripts moved or the "
            "export shape changed, the assertion below passes on an empty set"
            % (len(exported), EXPECTED_EXPORT_FLOOR))

    def test_every_exported_variable_is_read_somewhere(self) -> None:
        exported = _exported()
        dead = sorted(set(exported) - _mentioned_elsewhere(set(exported)))
        self.assertEqual(
            [], dead,
            "these are exported by a launcher and appear nowhere else, so every process the "
            "script starts carries a setting that cannot take:\n  %s\nRemove the export, or point "
            "it at the name something actually reads."
            % "\n  ".join("%s (%s)" % (name, ", ".join(exported[name])) for name in dead))


if __name__ == "__main__":
    unittest.main()
