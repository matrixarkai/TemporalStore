#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A portal field read only as a FALLBACK must say what wins over it.

Several knobs are read as `get(SPECIFIC, get(GENERAL, default))` -- a deliberate hierarchy, where a
provider-specific or path-specific name overrides a general one. The portal offers the GENERAL name
for three of them and said nothing about the specific one, so the field promised a reach it does
not have.

`retrieval.default_max_context_tokens` is the one that matters. It offers
`MATRIXARK_DEFAULT_MAX_CONTEXT_TOKENS` at 500000, while the agent hooks prefer
`MATRIXARK_HOOK_MAX_CONTEXT_TOKENS` -- which the installation manual instructs operators to set to
10000 -- and `matrixark_codex_dual_hook.sh` passes its own 10000 without consulting the portal's
variable at all. An operator raising the portal field and expecting a hook to send more got no
change and no explanation.

The rule is narrow on purpose: only a field whose OWN variable is the second name of such a pair
has to mention the first. It is not a demand that every setting document every neighbour.

This is the same defect as the one that had the portal advertising budgets it does not apply, in a
different disguise: a surface stating something the code will not do.
"""
from __future__ import annotations

import os
import re
import subprocess
import sys
import unittest
from typing import Dict

TOOLS = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(TOOLS)
sys.path.insert(0, TOOLS)

# get(SPECIFIC, get(GENERAL, ...)) -- the second name is only reached when the first is unset.
_PAIR = re.compile(
    r'environ\.get\(\s*["\']((?:TS|MATRIXARK|TEMPORALSTORE)_[A-Z0-9_]+)["\']\s*,\s*'
    r'(?:os\.)?(?:environ\.get|getenv)\(\s*["\']((?:TS|MATRIXARK|TEMPORALSTORE)_[A-Z0-9_]+)["\']')

# 10 pairs and 3 shadowed settings when this was written. The context-token budget was one of the
# three: the panel field is no longer read as a fallback behind the value the agent is given, so the
# population is 2 and the floor follows it down. Lowering a floor is only honest when an instance
# was FIXED -- if this drops again, check that the scan still matches before moving it.
EXPECTED_PAIR_FLOOR = 6
EXPECTED_SHADOWED_FLOOR = 2


def _pairs() -> Dict[str, str]:
    """fallback name -> the name preferred over it."""
    listed = subprocess.run(["git", "ls-files", "*.py"], cwd=REPO,
                            capture_output=True, text=True).stdout.split()
    found: Dict[str, str] = {}
    for path in listed:
        if os.path.basename(path).startswith("test_"):
            continue
        try:
            with open(os.path.join(REPO, path), encoding="utf-8", errors="replace") as handle:
                source = handle.read()
        except OSError:
            continue
        for match in _PAIR.finditer(source):
            found.setdefault(match.group(2), match.group(1))
    return found


def _shadowed():
    """(setting, the name preferred over its variable) for every field read as a fallback."""
    import matrixark_gateway_config as cfgmod
    pairs = _pairs()
    offered = {s.env for s in cfgmod.SETTINGS if s.env}
    out = []
    for setting in cfgmod.SETTINGS:
        preferred = pairs.get(setting.env)
        # If the portal also offers the preferred name, an operator can reach both and the
        # precedence is visible on the page itself.
        if preferred and preferred not in offered:
            out.append((setting, preferred))
    return out


class ASettingSaysWhatOverridesItTest(unittest.TestCase):

    def test_the_scan_still_finds_fallback_pairs(self) -> None:
        pairs = _pairs()
        self.assertGreaterEqual(
            len(pairs), EXPECTED_PAIR_FLOOR,
            "found %d primary/fallback pairs, expected at least %d -- if the read shape changed, "
            "the assertion below runs on an empty set" % (len(pairs), EXPECTED_PAIR_FLOOR))

    def test_the_scan_still_finds_shadowed_settings(self) -> None:
        shadowed = _shadowed()
        self.assertGreaterEqual(
            len(shadowed), EXPECTED_SHADOWED_FLOOR,
            "found %d portal fields read as a fallback, expected at least %d -- below that this "
            "file is asserting almost nothing" % (len(shadowed), EXPECTED_SHADOWED_FLOOR))

    def test_a_shadowed_field_names_what_wins_over_it(self) -> None:
        silent = ["%s (overridden by %s)" % (setting.key, preferred)
                  for setting, preferred in _shadowed() if preferred not in setting.help]
        self.assertEqual(
            [], silent,
            "these portal fields are read only when another variable is unset, and their help "
            "does not name it: %s\nAn operator changing one of these can get no effect and no "
            "explanation." % silent)


if __name__ == "__main__":
    unittest.main()
