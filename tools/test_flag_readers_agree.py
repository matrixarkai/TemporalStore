#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Two readers of one variable must agree about it.

A flag read in two places can disagree in three ways, and each produces a setting that half-applies:

  - the DEFAULT differs, so the answer depends on which module was imported
  - the accepted SPELLINGS differ, so the two agree on "1" and part company on "on"
  - the SENSE differs, one asking `in {...}` and the other `not in {...}`

Half-applied is worse than not applied, because each half looks correct where it is written and
nothing reports the disagreement. `MATRIXARK_ALLOW_LOCAL_BACKEND` was in exactly that state: the
hook accepted "on" and the production guard that refuses a local backend did not, so
`MATRIXARK_ALLOW_LOCAL_BACKEND=on` permitted the backend in one place and was refused in the other.

The statement is the unit, not a window of lines. A first version of this scan read three lines
from each match and reported four disagreements, of which three were its own: it took the spellings
of the NEXT line's different variable, and it truncated a set that continued past the window. A
statement carries its own set and nothing else's, which is why the scan below follows brackets.
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
    r'os\.(?:environ\.get|getenv)\(\s*["\']([A-Z][A-Z0-9_]{3,})["\']\s*,\s*["\']([^"\']*)["\']')
_SPELLING = re.compile(r'["\'](1|0|true|false|yes|no|on|off)["\']', re.I)
_NUMERIC = re.compile(r"\b(?:int|float)\s*\(")
_OTHER_FLAG = re.compile(r'["\']([A-Z][A-Z0-9_]{3,})["\']')
# The same read, once it goes through the one parser. Sites are being moved onto `env_bool`, and a
# scan that only knew the hand-rolled shape would watch its own population drain away and then
# report that every remaining reader agrees. Two of the three disagreements cannot occur here --
# `env_bool` fixes the spellings and the sense for every caller -- but the DEFAULT is still written
# at each site, so two readers of one flag can still disagree about what unset means.
_ENV_BOOL = re.compile(
    r'env_bool\(\s*["\']([A-Z][A-Z0-9_]{3,})["\']\s*,\s*(True|False)\s*\)')
ONE_VOCABULARY = ("<env_bool>",)

# Twenty-one when this was written. Asserted so a scan that stops matching fails rather than
# reporting that every reader agrees.
EXPECTED_SHARED_FLOOR = 15


def _production_sources() -> List[str]:
    listed = subprocess.run(["git", "ls-files", "*.py"], cwd=REPO,
                            capture_output=True, text=True).stdout.split()
    return [path for path in listed if not os.path.basename(path).startswith("test_")]


def _statement_at(lines: List[str], start: int) -> str:
    """The whole statement beginning at `start`, followed by bracket balance."""
    depth, collected = 0, []
    for number in range(start, min(start + 40, len(lines))):
        line = lines[number]
        collected.append(line)
        depth += line.count("(") + line.count("{") + line.count("[")
        depth -= line.count(")") + line.count("}") + line.count("]")
        if depth <= 0:
            break
    return "\n".join(collected)


def _readers() -> Dict[str, List[Tuple[str, int, str, Tuple[str, ...], bool]]]:
    found: Dict[str, List[Tuple[str, int, str, Tuple[str, ...], bool]]] = collections.defaultdict(list)
    for path in _production_sources():
        try:
            with open(os.path.join(REPO, path), encoding="utf-8") as handle:
                lines = handle.read().splitlines()
        except OSError:
            continue
        for number, line in enumerate(lines):
            for match in _READ.finditer(line):
                name, default = match.group(1), match.group(2).strip()
                statement = _statement_at(lines, number)
                if _NUMERIC.search(statement):
                    continue
                # A statement naming a second flag is not a clean read of either.
                if set(_OTHER_FLAG.findall(statement)) - {name}:
                    continue
                accepted = tuple(sorted({s.lower() for s in _SPELLING.findall(statement)}
                                        - {default.lower()}))
                if not accepted:
                    continue
                found[name].append((path, number + 1, default.lower(), accepted,
                                    "not in" in statement))
            for match in _ENV_BOOL.finditer(line):
                name, default = match.group(1), match.group(2)
                found[name].append((path, number + 1, default.lower(), ONE_VOCABULARY, False))
    return found


class TwoReadersOfOneFlagAgreeTest(unittest.TestCase):

    @staticmethod
    def _shared():
        return {name: entries for name, entries in _readers().items()
                if len({(entry[0], entry[1]) for entry in entries}) > 1}

    def test_the_scan_still_finds_shared_flags(self) -> None:
        shared = self._shared()
        self.assertGreaterEqual(
            len(shared), EXPECTED_SHARED_FLOOR,
            "found %d flags read from more than one production site, expected at least %d -- if "
            "the read shape changed, this file is looking for something that no longer exists and "
            "the assertion below passes on an empty set" % (len(shared), EXPECTED_SHARED_FLOOR))

    def test_no_two_readers_disagree(self) -> None:
        disagreeing = []
        for name, entries in sorted(self._shared().items()):
            ways = []
            if len({entry[2] for entry in entries}) > 1:
                ways.append("default")
            if len({entry[3] for entry in entries}) > 1:
                ways.append("spellings")
            if len({entry[4] for entry in entries}) > 1:
                ways.append("sense")
            if not ways:
                continue
            sites = "; ".join(
                "%s:%d default=%r %s{%s}"
                % (os.path.basename(entry[0]), entry[1], entry[2],
                   "not in " if entry[4] else "in ", ",".join(entry[3]))
                for entry in sorted(entries))
            disagreeing.append("%s disagrees on %s -- %s" % (name, "+".join(ways), sites))
        self.assertEqual(
            [], disagreeing,
            "a flag means different things to different readers, so setting it applies in some "
            "places and not others:\n  %s\nGive it one answer. Where the readers guard something, "
            "take the narrower spelling: a permission should not gain accepting spellings."
            % "\n  ".join(disagreeing))


if __name__ == "__main__":
    unittest.main()
