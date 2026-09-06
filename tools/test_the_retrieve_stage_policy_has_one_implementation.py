#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""The retrieve deadline policy has ONE implementation.

`matrixark_local_adapter_retrieve` restated, character for character, both halves of the policy
that `matrixark_mcp_retrieve_planning` already exports: the deadline resolution -- args, then
ranking, then the environment, then 0 -- and the stage-budget split that decides how much of a
deadline each stage may spend. Nineteen literals in two places. The comment above them in the
adapter already said the audit policy had been consolidated "rather than a duplicated literal";
these two were left.

Two copies of a policy agree until one is edited, and this one is invisible when it drifts: the
budgets are reported, not enforced, so a wrong split shows up as telemetry that quietly stops
matching the path it describes.

The check is on the SPLIT rather than on an import, because an import can be present while a
second copy sits beside it. The proportions are what must exist once.
"""
from __future__ import annotations

import os
import re
import subprocess
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(TOOLS)

#: The proportional split, in the order the policy writes it. Matched as a sequence so a module
#: that merely mentions 0.15 somewhere is not accused of restating the policy.
SPLIT = ["0.15", "0.20", "0.15", "0.30", "0.15", "0.05"]

#: The absolute budgets used when a request carries no deadline.
FLOOR = ["500", "750", "500", "1000", "500", "250"]

#: The module the policy belongs to.
OWNER = "tools/matrixark_mcp_retrieve_planning.py"


def _production_sources() -> list:
    listed = subprocess.run(["git", "ls-files", "tools/*.py"], cwd=REPO,
                            capture_output=True, text=True).stdout.split()
    return [path for path in listed if not os.path.basename(path).startswith("test_")]


def _modules_carrying(sequence: list) -> list:
    # Decimals and integers are scanned SEPARATELY. The split is written as
    # max(25, int(deadline_ms * 0.15)), so a scan over every numeric token sees the 25s
    # interleaved and never finds the proportions next to each other -- which is what the extent
    # assertion below caught when this was written against one pattern.
    decimal = "." in sequence[0]
    pattern = re.compile(r"\d+\.\d+" if decimal else r"(?<![\d.])\d+(?![\d.])")
    carrying = []
    for path in _production_sources():
        try:
            with open(os.path.join(REPO, path), encoding="utf-8", errors="replace") as handle:
                numbers = pattern.findall(handle.read())
        except OSError:
            continue
        for start in range(len(numbers) - len(sequence) + 1):
            if numbers[start:start + len(sequence)] == sequence:
                carrying.append(path)
                break
    return sorted(carrying)


class TheRetrieveStagePolicyHasOneImplementation(unittest.TestCase):

    def test_the_owner_still_carries_the_split(self) -> None:
        """Assert this scan's own extent: if the policy is rewritten, the checks below are empty."""
        self.assertIn(OWNER, _modules_carrying(SPLIT),
                      "the proportional split is no longer in %s, so this file is asserting that "
                      "nothing restates a policy it can no longer find" % OWNER)
        self.assertIn(OWNER, _modules_carrying(FLOOR),
                      "the no-deadline budgets are no longer in %s" % OWNER)

    def test_only_one_module_carries_the_proportional_split(self) -> None:
        carrying = _modules_carrying(SPLIT)
        self.assertEqual(
            [OWNER], carrying,
            "the stage-budget split is written in more than one place: %s. It decides how much of "
            "a deadline each stage may spend, and the budgets are REPORTED rather than enforced, "
            "so a copy that drifts shows up as telemetry that stops matching the path it "
            "describes. Call matrixark_mcp_retrieve_planning.default_stage_budgets instead."
            % carrying)

    def test_only_one_module_carries_the_no_deadline_budgets(self) -> None:
        carrying = _modules_carrying(FLOOR)
        self.assertEqual(
            [OWNER], carrying,
            "the budgets used when a request carries no deadline are written in more than one "
            "place: %s" % carrying)


if __name__ == "__main__":
    unittest.main()
