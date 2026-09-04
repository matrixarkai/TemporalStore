#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""No new test module may import another test module at import time.

Under `unittest discover` a module is reachable as both `tools.X` and bare `X`, so importing one
test module from another pulls a second copy into the run and shifts what every later module sees.
The ordering that produces is environment-dependent.

This cost an afternoon once and is worth the guard. A new test module imported the gateway suite's
fixtures at its top, and CI's ratchet reported **five failing tests the branch had nothing to do
with** -- a snapshot reader, a batch extractor, and two that parse a shipped `prometheus.yml` the
branch never touched. Locally the full suite gave the identical failing set on the branch and on
main, twice: 118 names, zero difference. Moving that one import into `setUp` made the ratchet pass
with zero new failures.

The symptom is specific enough to name: **CI fails on tests your diff cannot explain, and you
cannot reproduce it locally.** When that happens, look here before investigating the tests.

Fourteen modules already do this and are recorded below rather than changed. They are baked into
the current baseline, and rewriting fourteen working modules to fix a hazard that has not bitten
them would be a large change with its own ordering risk. So this ratchets: nothing new may appear,
and anything fixed must be struck from the list, which is what stops the list becoming furniture.

The fix, when writing a new one: import the other module's fixtures inside `setUp` rather than at
module level.
"""
from __future__ import annotations

import ast
import io
import os
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))

# Modules that already import another test module at import time. This list may only shrink.
KNOWN = {
    "test_matrixark_deployment_routes.py",
    "test_matrixark_embedding_status.py",
    "test_matrixark_every_live_claim_is_checked.py",
    "test_matrixark_gateway_events.py",
    "test_matrixark_gateway_portal.py",
    "test_matrixark_gateway_routes.py",
    "test_matrixark_ingest_file_scope.py",
    "test_matrixark_ingestion_retry.py",
    "test_matrixark_live_frames_are_shared.py",
    "test_matrixark_live_strip.py",
    "test_matrixark_mem0_console.py",
    "test_matrixark_models.py",
    "test_matrixark_readiness_sources.py",
    "test_matrixark_user_policy.py",
}


def _cross_importers() -> dict:
    """Test modules importing another test module at MODULE level, and what they import.

    Module level only. An import inside a function runs when the test runs, by which point the
    suite's module set is already decided -- that is the whole point of the fix this guards.
    """
    found = {}
    for name in sorted(os.listdir(TOOLS)):
        if not (name.startswith("test_") and name.endswith(".py")):
            continue
        try:
            tree = ast.parse(io.open(os.path.join(TOOLS, name), encoding="utf-8").read())
        except SyntaxError:
            continue
        hits = []
        for node in tree.body:
            if isinstance(node, ast.ImportFrom) and (node.module or "").startswith("test_"):
                hits.append(node.module)
            elif isinstance(node, ast.Import):
                hits += [alias.name for alias in node.names if alias.name.startswith("test_")]
        if hits:
            found[name] = sorted(set(hits))
    return found


class NoNewCrossTestImportsTest(unittest.TestCase):

    def setUp(self) -> None:
        self.found = _cross_importers()

    def test_no_module_outside_the_recorded_list_does_it(self) -> None:
        added = sorted(set(self.found) - KNOWN)
        self.assertEqual(
            [], added,
            "these import another test module at import time, which reorders `unittest discover` "
            "and can fail tests they have nothing to do with -- in CI, while passing locally. "
            "Import the fixtures inside setUp instead: %s"
            % ", ".join("%s (%s)" % (name, ", ".join(self.found[name])) for name in added))

    def test_the_list_has_no_entries_that_no_longer_apply(self) -> None:
        """A ratchet that only ever grows stops being a ratchet."""
        stale = sorted(KNOWN - set(self.found))
        self.assertEqual([], stale,
                         "these no longer import a test module and should be struck from KNOWN: %s"
                         % ", ".join(stale))

    def test_the_guard_is_looking_at_a_real_tree(self) -> None:
        """A scan that finds no test modules would pass this file and prove nothing."""
        modules = [n for n in os.listdir(TOOLS)
                   if n.startswith("test_") and n.endswith(".py")]
        self.assertGreater(len(modules), 100,
                           "only %d test modules found; the scan is not reaching the tree"
                           % len(modules))

    def test_this_module_does_not_do_it_itself(self) -> None:
        self.assertNotIn(os.path.basename(__file__), self.found)


if __name__ == "__main__":
    unittest.main()
