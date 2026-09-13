#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A dict literal that names the same key twice silently drops the first value.

Python does not warn. `{"a": 1, "a": 2}` is `{"a": 2}`, and if the two values differ the first one
is gone with nothing to show for it -- a configured value that never takes effect, and an edit to
the wrong line that changes nothing at runtime.

Three existed when this was written, and all three happened to name the SAME value twice, so
nothing was being lost yet:

    run_codex_history_oss_context_benchmark   "native" twice, same value
    run_local_context_token_quality_sweep     "native" twice, same value
    run_fair_oss_benchmark_suite              "external_baseline_stack" twice, identical blocks

They are removed in the commit that adds this file. The value of the guard is not those three --
it is the fourth one, where the values differ and the loss is silent.

The denominator is asserted, because a scan that stops matching dict literals would pass this file
while seeing nothing: there are about 7,900 dict literals with two or more constant keys in the
tree, so the floor is set well below that and still far above zero.

Scope note: only literal constant keys are checked. `{**a, **b}` and computed keys legitimately
override, and a duplicate there is a real pattern rather than a mistake.
"""
from __future__ import annotations

import ast
import os
import subprocess
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(TOOLS)


def _scan():
    """(duplicates, number of dict literals with >= 2 constant keys)."""
    out = subprocess.run(["git", "ls-files", "tools/*.py"], cwd=ROOT,
                         capture_output=True, text=True).stdout.split()
    duplicates, considered = [], 0
    for rel in out:
        try:
            with open(os.path.join(ROOT, rel), encoding="utf-8", errors="replace") as handle:
                tree = ast.parse(handle.read())
        except SyntaxError:  # pragma: no cover - an unparseable module fails elsewhere
            continue
        for node in ast.walk(tree):
            if not isinstance(node, ast.Dict):
                continue
            literal = [k for k in node.keys
                       if isinstance(k, ast.Constant) and isinstance(k.value, (str, int, bool))]
            if len(literal) < 2:
                continue
            considered += 1
            seen = set()
            for key in literal:
                if key.value in seen:
                    duplicates.append((os.path.basename(rel), key.lineno, repr(key.value)))
                seen.add(key.value)
    return duplicates, considered


class ALiteralDoesNotNameAKeyTwice(unittest.TestCase):

    def test_the_scan_is_actually_reading_literals(self) -> None:
        """A floor on the DENOMINATOR.

        Without it, a scan that stopped matching dict literals would report zero duplicates and
        look like a clean tree.
        """
        _duplicates, considered = _scan()
        self.assertGreater(
            considered, 3000,
            "only %d dict literals with two or more constant keys were seen, so this file is "
            "checking almost nothing" % considered)

    def test_no_literal_names_the_same_key_twice(self) -> None:
        duplicates, _considered = _scan()
        self.assertEqual(
            [], duplicates,
            "these dict literals name a key twice, so the first value is silently discarded: %s"
            % ", ".join("%s:%d %s" % row for row in duplicates))


if __name__ == "__main__":
    unittest.main()
