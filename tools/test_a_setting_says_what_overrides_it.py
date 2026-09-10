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

import ast
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


def _unreachable_modules() -> set:
    """The modules only the tests reach, from the guard that maintains that list.

    Imported rather than restated: a second copy of forty-three module names would go stale exactly
    the way the duplicated scope tables did.
    """
    import importlib.util

    path = os.path.join(TOOLS, "test_a_module_only_tests_reach_is_not_live.py")
    spec = importlib.util.spec_from_file_location("_reachability_for_settings", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    out: set = set()
    for members in module.UNREACHABLE.values():
        out.update(members)
    return out


def _flags_named_by_live_modules() -> set:
    """Every MATRIXARK_*/TS_* name appearing as a string constant in a module a request can reach.

    Deliberately looser than "is read": a helper of the module's own, a name passed to something
    else, a name in a table. The question here is not how a live module uses the variable but
    whether any live module knows it exists at all -- and for the sixteen this pins, none does.
    """
    unreachable = _unreachable_modules()
    listed = subprocess.run(["git", "ls-files", "tools/*.py"], cwd=REPO,
                            capture_output=True, text=True, check=False).stdout.split()
    found: set = set()
    for rel in listed:
        stem = os.path.basename(rel)[:-3]
        if stem.startswith("test_") or stem in unreachable:
            continue
        if stem == "matrixark_gateway_config":
            # The module that DECLARES the settings is excluded, or the declaration is its own
            # evidence: adding `Setting(..., "MATRIXARK_X", ...)` puts MATRIXARK_X into this set,
            # and the question "does anything live name MATRIXARK_X" then answers itself. A
            # mutation adding a Setting for a flag only unreachable modules name PASSED before
            # this line. It is the question, not the answer.
            continue
        try:
            with open(os.path.join(REPO, rel), encoding="utf-8", errors="replace") as handle:
                tree = ast.parse(handle.read())
        except (OSError, SyntaxError):
            continue
        for node in ast.walk(tree):
            if isinstance(node, ast.Constant) and isinstance(node.value, str) \
                    and node.value.startswith(("MATRIXARK_", "TS_")):
                found.add(node.value)
    return found


def _flags_named_by_the_engine() -> set:
    """Names the Rust engine reads. Most TS_* settings have no Python reader by design."""
    out = subprocess.run(["git", "grep", "-h", "-o", "-E", "(MATRIXARK|TS)_[A-Z0-9_]+",
                          "--", "crates/"], cwd=REPO, capture_output=True, text=True,
                         check=False).stdout.split()
    return set(out)


def _declared_setting_variables() -> dict:
    """{env variable: dotted key} for every Setting the portal offers."""
    with open(os.path.join(TOOLS, "matrixark_gateway_config.py"), encoding="utf-8",
              errors="replace") as handle:
        tree = ast.parse(handle.read())
    declared = {}
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or getattr(node.func, "id", "") != "Setting":
            continue
        if len(node.args) < 3:
            continue
        try:
            key = ast.literal_eval(node.args[0])
            variable = ast.literal_eval(node.args[2])
        except (ValueError, SyntaxError):
            continue
        if isinstance(variable, str) and variable:
            declared[variable] = key
    return declared


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




class ASettingOffersAVariableSomethingLiveReadsTest(unittest.TestCase):
    """A field whose variable nothing on a live path names is a control that changes nothing."""

    def test_every_declared_setting_reaches_code_that_serves_a_request(self) -> None:
        """The rule. A Setting is an offer to an operator, so the variable behind it has to be
        known to something a request reaches -- a live Python module, or the Rust engine, which is
        where most TS_* settings are read and why a Python-only scan would report them wrongly."""
        declared = _declared_setting_variables()
        known = _flags_named_by_live_modules() | _flags_named_by_the_engine()
        stranded = sorted((variable, key) for variable, key in declared.items()
                          if variable not in known)
        self.assertEqual(
            [], stranded,
            "these settings are offered to operators and no live module or engine source names "
            "the variable, so setting them changes nothing: %r" % (stranded,))

    def test_the_scan_finds_the_settings_and_the_live_names(self) -> None:
        """A floor. With either side empty the check above passes over nothing."""
        self.assertGreater(
            len(_declared_setting_variables()), 150,
            "the Setting scan came back nearly empty")
        self.assertGreater(
            len(_flags_named_by_live_modules()), 300,
            "the live-module flag scan came back nearly empty, so every setting would look "
            "stranded or none would")
        self.assertGreater(
            len(_unreachable_modules()), 30,
            "the reachability list came back nearly empty, so nothing would count as unreachable "
            "and the check would be about the wrong set")

    def test_a_flag_only_unreachable_modules_name_is_not_treated_as_live(self) -> None:
        """The discriminator, checked against a real example rather than a contrived one.

        `MATRIXARK_RUST_PROXY_APPEND_COALESCE` is named only in the rust-proxy config and client,
        which only tests reach. If the live scan started counting those modules, the check above
        would accept a setting for it -- and pass while offering exactly the control it exists to
        refuse."""
        self.assertNotIn(
            "MATRIXARK_RUST_PROXY_APPEND_COALESCE", _flags_named_by_live_modules(),
            "a flag named only by modules a request cannot reach is being counted as live")


if __name__ == "__main__":
    unittest.main()
