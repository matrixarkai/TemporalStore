#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A default-ON switch that nothing can turn off is a branch, not a switch.

The engine's flags are inventoried and guarded. The Python side was not, and it had accumulated
eleven variables that defaulted on and that nothing anywhere selected the other position of: no
test, no shell script, no config file, no tenant knob, no portal control, no command-line argument.
Each kept a second code path alive that nothing could reach, and two of them were read in three
places each, where the copies had already begun to disagree.

They were retired by keeping the live path. This holds the line: a NEW one fails here.

What counts as a selector is deliberately wide, because the ways to set one of these are not all
code. A person setting a tenant knob or a portal control is a selector no search for an assignment
finds, and this check would be wrong -- dangerously so, since it recommends deleting a customer's
control -- if it looked only for assignments in this repository.

Listing is the escape, not an exception. An entry has to say why the switch cannot be reached, and
the last test fails if a listed switch becomes reachable, so a reason that stops being true does
not quietly stand.
"""
from __future__ import annotations

import os
import re
import subprocess
import unittest
from typing import Dict, List, Set, Tuple

TOOLS = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(TOOLS)

# A read whose value is turned into a bool, with a default that makes the switch default-ON.
_READ = re.compile(
    r'os\.(?:environ\.get|getenv)\(\s*["\']([A-Z][A-Z0-9_]{3,})["\']\s*,\s*["\']([^"\']*)["\']')
_TRUTH_SET = re.compile(r'in\s*[\(\{\[][^)\}\]]*["\'](?:1|true|yes|on|0|false|no|off)["\']', re.I)
_EQ = re.compile(r'[=!]=\s*["\'](?:1|true|yes|on|0|false|no|off)["\']', re.I)
_NUMERIC = re.compile(r'\b(?:int|float)\s*\(')
_ON_DEFAULT = re.compile(r'^(?:1|true|yes|on)$', re.I)

# The two files that OFFER settings rather than set them. In these, naming the variable at all is
# the offer: the value arrives from a customer, and the declaration spans lines, so no assignment
# pattern finds it. MATRIXARK_LOCAL_DURABLE_READ_CACHE_ENABLED is declared here as the third
# argument of a Setting(...) on a line of its own, and reading it as unreachable would have
# recommended deleting a control the gateway offers.
_REGISTRIES = ("matrixark_gateway_config.py", "matrixark_tenant_policy.py")

# Each of these defaults ON and nothing selects the other position. The entry says why that is the
# right answer rather than a defect.
KNOWN_UNSELECTED: Dict[str, str] = {
    # Gates schema DDL against a live database. Turning it off is what an operator running against
    # a managed schema does, and that operator is outside this repository by construction.
    "MATRIXARK_METADATA_AUTO_INIT": "operator safety switch for schema creation",
    # A benchmark harness's own parameter: the person running the harness is the selector, exactly
    # as with the engine's benchmark knobs.
    "MATRIXARK_REQUIRE_SHARED_OSS_MODELS": "benchmark harness parameter, set by whoever runs it",
    "TEMPORALSTORE_HF_READER_PRELOAD": "local model server parameter, set by whoever runs it",
}

# Seven remain after the eleven were retired. The floor is asserted so a scan that stops matching
# fails rather than reporting that all is well.
EXPECTED_SWITCH_FLOOR = 5


def _production_sources() -> List[str]:
    listed = subprocess.run(["git", "ls-files", "*.py"], cwd=REPO,
                            capture_output=True, text=True).stdout.split()
    return [path for path in listed if not os.path.basename(path).startswith("test_")]


def _default_on_switches() -> Dict[str, str]:
    """Variable -> "path:line" of the first read that makes it a default-ON boolean switch."""
    found: Dict[str, str] = {}
    for path in _production_sources():
        try:
            with open(os.path.join(REPO, path), encoding="utf-8") as handle:
                lines = handle.read().splitlines()
        except OSError:
            continue
        for number, line in enumerate(lines, 1):
            for match in _READ.finditer(line):
                name, default = match.group(1), match.group(2)
                if name in found or not _ON_DEFAULT.match(default.strip()):
                    continue
                window = " ".join(lines[number - 1:number + 2])
                if _NUMERIC.search(window):
                    continue
                if not (_TRUTH_SET.search(window) or _EQ.search(window)):
                    continue
                found[name] = "%s:%d" % (path, number)
    return found


def _selector_texts() -> List[Tuple[bool, str]]:
    """(is_registry, text) for everywhere a switch could be selected.

    This file is excluded, and the reason is worth keeping. KNOWN_UNSELECTED writes each name as
    a quoted key followed by a colon, which is one of the shapes that counts as selecting -- so
    once this file was committed it read its own list as a set of selectors and reported every
    listed switch as reachable. It could not do that before being committed, because the scan
    walks  and an untracked file is not listed: the test was green when written and
    red when merged. A file that LISTS switches must never count as selecting them.
    """
    listed = subprocess.run(
        ["git", "ls-files", "*.py", "*.sh", "*.json", "*.toml", "*.yml", "*.yaml", "*.rs", "*.js"],
        cwd=REPO, capture_output=True, text=True).stdout.split()
    myself = os.path.basename(__file__)
    texts: List[Tuple[bool, str]] = []
    for path in listed:
        if os.path.basename(path) == myself:
            continue
        try:
            with open(os.path.join(REPO, path), encoding="utf-8") as handle:
                texts.append((os.path.basename(path) in _REGISTRIES, handle.read()))
        except OSError:
            continue
    return texts


def _selected(names: Set[str]) -> Set[str]:
    """Names something gives a value to, as opposed to reading."""
    patterns = {}
    for name in names:
        escaped = re.escape(name)
        patterns[name] = re.compile(
            r"export\s+" + escaped + r"="
            r"|environ\[\s*[\"']" + escaped + r"[\"']\s*\]\s*="
            r"|setdefault\(\s*[\"']" + escaped + r"[\"']"
            r"|set_var\(\s*\"" + escaped + r"\""
            r"|[\"']" + escaped + r"[\"']\s*:")
    selected: Set[str] = set()
    for is_registry, text in _selector_texts():
        for name, pattern in patterns.items():
            if name in selected:
                continue
            if pattern.search(text):
                selected.add(name)
            elif is_registry and ('"' + name + '"') in text:
                selected.add(name)
    return selected


class ADefaultOnSwitchIsReachableOrListedTest(unittest.TestCase):

    def test_the_census_still_finds_switches(self) -> None:
        switches = _default_on_switches()
        self.assertGreaterEqual(
            len(switches), EXPECTED_SWITCH_FLOOR,
            "found %d default-ON boolean switches, expected at least %d -- if the read shape "
            "changed, this file is looking for something that no longer exists and the assertion "
            "below passes on an empty set" % (len(switches), EXPECTED_SWITCH_FLOOR))

    def test_the_list_has_not_emptied(self) -> None:
        self.assertTrue(
            KNOWN_UNSELECTED,
            "the list is empty, so the check below cannot tell a clean tree from a broken scan. "
            "Delete this file rather than leave it passing on nothing.")

    def test_no_new_switch_is_unreachable(self) -> None:
        switches = _default_on_switches()
        unreachable = set(switches) - _selected(set(switches)) - set(KNOWN_UNSELECTED)
        new = sorted("%s (%s)" % (name, switches[name]) for name in unreachable)
        self.assertEqual(
            [], new,
            "these default ON and nothing selects the other position -- not a test, a shell "
            "script, a config file, a tenant knob, a portal control or a command-line argument. "
            "The path behind them cannot be reached: %s. Make the live path unconditional, or "
            "give the setting to the object it is about, or list it here with the reason." % new)

    def test_a_listed_switch_that_became_reachable_is_struck_off(self) -> None:
        switches = _default_on_switches()
        listed = set(KNOWN_UNSELECTED) & set(switches)
        reachable = sorted(listed & _selected(set(switches)))
        self.assertEqual(
            [], reachable,
            "listed as unreachable, but something now selects them: %s. Strike them off -- a "
            "reason that has stopped being true is worse than no reason." % reachable)


if __name__ == "__main__":
    unittest.main()
