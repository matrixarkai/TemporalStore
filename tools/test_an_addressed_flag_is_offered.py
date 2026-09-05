#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A flag whose comment tells an operator what to do must give them somewhere to do it.

`MATRIXARK_LOCAL_DURABLE_READ_CACHE_MIN_WRITE_MS` is the worked example. Its comment measured
the trade in both directions -- a floor of 5000 removed all 12 base rewrites and took the
median query from 2,352 ms to 1,922 ms, at a cold load rising from 1,391 ms to 1,507 ms --
and concluded that the choice belongs to whoever runs the deployment rather than to a
default. It then appeared on no portal page and in no shipped config file, so the only way
to act on that paragraph was to edit the process environment of a running box.

Its sibling `MATRIXARK_LOCAL_DURABLE_READ_CACHE_ENABLED` had been on the portal all along.
The switch that turned the snapshot off was adjustable; the knob deciding what keeping it on
costs was not. Nothing announced that, because both halves were correct in isolation.

So the property is not "every flag is on the portal" -- most flags are internal and should
stay that way. It is narrower: **if the code writes a sentence addressed to an operator, the
operator can reach the knob that sentence is about.** Advice with nowhere to land is worse
than no advice, because it reads as though the surface exists.

Two decisions keep this from crying wolf:

* "Offered" is generous. A name counts if it appears ANYWHERE in the gateway config, the
  tenant policy, or any shipped toml/yaml/json -- whatever the shape, without checking that
  it is wired to a control. Over-counting offers means this under-reports, and under-reporting
  is the direction that does not send anyone chasing a knob that is already there.
* "Addressed" is a short, literal list of second-person phrasings. It does not try to judge
  tone. A comment that explains a trade without telling anyone to act on it is not a finding.
"""
from __future__ import annotations

import os
import pathlib
import re
import subprocess
import unittest
from typing import Dict, Set, Tuple

TOOLS = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(TOOLS)

_READ = re.compile(
    r'os\.(?:environ\.get|getenv)\(\s*["\']((?:TS|MATRIXARK|TEMPORALSTORE)_[A-Z0-9_]+)["\']'
    r'|os\.environ\[\s*["\']((?:TS|MATRIXARK|TEMPORALSTORE)_[A-Z0-9_]+)["\']')

_NAME = re.compile(r"(?:TS|MATRIXARK|TEMPORALSTORE)_[A-Z0-9_]+")

# Second person, or an explicit hand-over of the decision. Deliberately literal.
_ADDRESSED = re.compile(
    r"\bturn (?:it|this) (?:on|off)\b"
    r"|\bset (?:this|it) (?:if|when|to)\b"
    r"|\bworth it for\b"
    r"|\ban operator\b|\boperator's call\b"
    r"|\bfor most deployments\b|\byour deployment\b"
    r"|\benable (?:this|it) (?:if|when)\b"
    r"|\bleave (?:this|it) (?:on|off|alone)\b",
    re.IGNORECASE)

_OFFER_FILES = ("tools/matrixark_gateway_config.py", "tools/matrixark_tenant_policy.py")

# 451 distinct flags read and 221 reachable names when this was written. The floors exist because every
# assertion below passes on an empty scan, and a scan goes empty quietly.
EXPECTED_READ_FLOOR = 300
EXPECTED_OFFERED_FLOOR = 150


def _tracked(*globs: str) -> list:
    return subprocess.run(["git", "ls-files", *globs], cwd=REPO,
                          capture_output=True, text=True).stdout.split()


def _text(rel: str) -> str:
    try:
        return (pathlib.Path(REPO) / rel).read_text(encoding="utf-8", errors="replace")
    except OSError:
        return ""


def _offered() -> Set[str]:
    names: Set[str] = set()
    for rel in _OFFER_FILES:
        names |= set(_NAME.findall(_text(rel)))
    for rel in _tracked("*.toml", "*.yaml", "*.yml", "*.json"):
        names |= set(_NAME.findall(_text(rel)))
    return names


def _reads() -> Dict[str, Tuple[str, int, str]]:
    """Every flag read in production Python, with the comment block written above it."""
    found: Dict[str, Tuple[str, int, str]] = {}
    for rel in _tracked("*.py"):
        if os.path.basename(rel).startswith("test_"):
            continue
        lines = _text(rel).splitlines()
        for number, line in enumerate(lines, 1):
            match = _READ.search(line)
            if not match:
                continue
            name = match.group(1) or match.group(2)
            if name in found:
                continue
            block = []
            index = number - 2
            while index >= 0 and lines[index].lstrip().startswith("#"):
                block.append(lines[index].lstrip().lstrip("#").strip())
                index -= 1
            block.reverse()
            found[name] = (rel, number, " ".join(block))
    return found


def _unreachable_advice() -> Dict[str, Tuple[str, int, str]]:
    offered = _offered()
    out = {}
    for name, (rel, number, prose) in _reads().items():
        if name in offered:
            continue
        match = _ADDRESSED.search(prose)
        if match:
            out[name] = (rel, number, match.group(0))
    return out


class AnAddressedFlagIsOfferedTest(unittest.TestCase):

    def test_the_scan_still_finds_flag_reads(self) -> None:
        reads = _reads()
        self.assertGreaterEqual(
            len(reads), EXPECTED_READ_FLOOR,
            "found %d distinct flags read, expected at least %d -- if the read shape changed, the "
            "assertion below passes on an empty set"
            % (len(reads), EXPECTED_READ_FLOOR))

    def test_the_scan_still_finds_the_surfaces(self) -> None:
        offered = _offered()
        self.assertGreaterEqual(
            len(offered), EXPECTED_OFFERED_FLOOR,
            "found %d names the operator can reach, expected at least %d -- if this collapses, "
            "every offered flag reads as unoffered and the check below floods"
            % (len(offered), EXPECTED_OFFERED_FLOOR))

    def test_the_phrasings_still_match_something(self) -> None:
        addressed = [name for name, (_, _, prose) in _reads().items()
                     if _ADDRESSED.search(prose)]
        self.assertTrue(
            addressed,
            "no flag comment matches any operator-addressed phrasing at all. Either the tree "
            "stopped writing them or the pattern stopped matching; both make this file inert.")

    def test_no_flag_gives_advice_the_operator_cannot_act_on(self) -> None:
        unreachable = _unreachable_advice()
        detail = ["%s (%s:%d, says %r)" % (name, rel, number, phrase)
                  for name, (rel, number, phrase) in sorted(unreachable.items())]
        self.assertEqual(
            [], detail,
            "these flags are described to an operator who has no way to reach them -- not the "
            "portal, not the tenant policy, not any shipped config, only the process "
            "environment of a running box: %s\nOffer it, or write the comment to a reader "
            "instead of an operator." % detail)


if __name__ == "__main__":
    unittest.main()
