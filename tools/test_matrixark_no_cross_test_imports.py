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
    "test_backend_policy_part1.py",
    "test_backend_policy_part2.py",
    "test_backend_policy_part3.py",
    "test_backend_policy_part4.py",
    "test_codex_hook_output_part2.py",
    "test_codex_hook_output_part3.py",
    "test_codex_pipeline_part1.py",
    "test_codex_pipeline_part2.py",
    "test_codex_pipeline_part3.py",
    "test_codex_pipeline_part4.py",
    "test_codex_pipeline_part5.py",
    "test_matrixark_codex_hook_output.py",
    "test_matrixark_codex_hook_pipeline.py",
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
    "test_matrixark_mcp_backend_policy.py",
    "test_matrixark_mem0_console.py",
    "test_matrixark_models.py",
    "test_matrixark_python_module_boundaries.py",
    "test_matrixark_readiness_sources.py",
    "test_matrixark_user_policy.py",
}

#: Pairs where each module imports the OTHER at import time. A one-directional cross-import
#: shifts what later modules see; a MUTUAL one decides whether either can be LOADED at all, and
#: the answer depends on what ran before it.
#:
#: Demonstrated on test_codex_hook_output_part2. Imported on its own it raises
#: "cannot import name '_CodexHookOutputPart2' from partially initialized module"; imported
#: after the sixty-four modules discovery loads ahead of it, it succeeds -- and the parent is
#: still not in sys.modules at that point. The suite is green because of an order, not because
#: the cycle is resolved.
#:
#: Recorded rather than changed, for the reason the list above is. What must not appear is a
#: fourteenth, because the order that rescues these is incidental and nothing asserts it.
#: Source for the control below, written out rather than escaped inline.
WRAPPED_SNIPPET = """try:
    from tools.test_x import Fixture
except ImportError:
    from test_x import Fixture
"""

DEFERRED_SNIPPET = """def setUp(self):
    from test_x import Fixture
    return Fixture
"""

KNOWN_MUTUAL = {
    ("test_backend_policy_part1", "test_matrixark_mcp_backend_policy"),
    ("test_backend_policy_part2", "test_matrixark_mcp_backend_policy"),
    ("test_backend_policy_part3", "test_matrixark_mcp_backend_policy"),
    ("test_backend_policy_part4", "test_matrixark_mcp_backend_policy"),
    ("test_codex_hook_output_part2", "test_matrixark_codex_hook_output"),
    ("test_codex_hook_output_part3", "test_matrixark_codex_hook_output"),
    ("test_codex_pipeline_part1", "test_matrixark_codex_hook_pipeline"),
    ("test_codex_pipeline_part2", "test_matrixark_codex_hook_pipeline"),
    ("test_codex_pipeline_part3", "test_matrixark_codex_hook_pipeline"),
    ("test_codex_pipeline_part4", "test_matrixark_codex_hook_pipeline"),
    ("test_codex_pipeline_part5", "test_matrixark_codex_hook_pipeline"),
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
        found_here = _module_level_test_imports(tree)
        if found_here:
            found[name] = sorted(found_here)
    return found


def _module_level_test_imports(tree) -> set:
    """Test modules imported at IMPORT TIME, including from inside a module-level try/except.

    This used to read `tree.body` alone, which is not the same question as the docstring above
    asks. Nearly every dual-path import in this tree is written

        try:  # package path
            from tools.test_x import Fixture
        except ImportError:
            from test_x import Fixture

    and a `try` at module level runs when the module is imported, exactly like a bare import. Only
    an import nested inside a FUNCTION or CLASS body is deferred, and that is the fix this guard
    prescribes -- so those, and only those, are excluded.

    Reading tree.body missed thirteen mutual pairs. It is the difference between asking "is this
    statement an import" and "does this import run at import time", and it under-reported silently.
    """
    deferred = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            for inner in ast.walk(node):
                if isinstance(inner, (ast.Import, ast.ImportFrom)):
                    deferred.add(id(inner))
    hits = set()
    for node in ast.walk(tree):
        if id(node) in deferred:
            continue
        if isinstance(node, ast.ImportFrom) and node.module:
            module = node.module.rsplit(".", 1)[-1]
            if module.startswith("test_"):
                hits.add(module)
        elif isinstance(node, ast.Import):
            for alias in node.names:
                module = alias.name.rsplit(".", 1)[-1]
                if module.startswith("test_"):
                    hits.add(module)
    return hits


class NoNewCrossTestImportsTest(unittest.TestCase):

    def test_the_scan_sees_a_try_wrapped_import(self) -> None:
        """The blind spot this scan had, kept as a control rather than a memory.

        It read `tree.body`, so it saw a bare top-level import and missed one inside a
        module-level try/except -- which is how nearly every dual-path import in this tree is
        written, and which runs at import time exactly the same. Seventeen cross-importers and
        all thirteen mutual pairs were invisible to it.

        The other half of the line matters too: an import inside a FUNCTION or CLASS is
        deferred, and deferring is the fix this guard prescribes. It must not be counted, or the
        remedy would fail the rule."""
        wrapped = ast.parse(WRAPPED_SNIPPET)
        self.assertEqual({"test_x"}, _module_level_test_imports(wrapped),
                         "a try-wrapped import at module level is no longer seen")
        deferred = ast.parse(DEFERRED_SNIPPET)
        self.assertEqual(set(), _module_level_test_imports(deferred),
                         "an import inside a function is being counted; that is the FIX, not "
                         "the defect")

    def test_no_new_pair_imports_each_other(self) -> None:
        """Mutual is worse than one-directional: it decides whether either module loads.

        Fails in both directions, like the list above -- a new pair, or one that stopped being
        mutual and should be struck."""
        found = _cross_importers()
        graph = {name[:-3]: set(mods) for name, mods in found.items()}
        pairs = {tuple(sorted((left, right))) for left, deps in graph.items()
                 for right in deps if right in graph and left in graph[right]}
        self.assertEqual(
            KNOWN_MUTUAL, pairs,
            "the set of test modules importing each other has changed. New: %s. Gone: %s -- "
            "strike those from KNOWN_MUTUAL, because a list allowed to go stale describes a "
            "tree that no longer exists."
            % (sorted(pairs - KNOWN_MUTUAL), sorted(KNOWN_MUTUAL - pairs)))

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
