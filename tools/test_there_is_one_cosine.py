# SPDX-License-Identifier: Apache-2.0
"""There is one cosine, and the retrieval path reaches it.

`cosine` was implemented twice. `matrixark_mcp_scoring.cosine` divides by both norms and carries a
docstring explaining why: the bare dot product is the same number as a cosine while both sides are
unit vectors and a different one as soon as either is not, and every vector-compaction option
produces a non-unit vector. `matrixark_mcp_core.cosine` was the bare dot product, and never got
that fix.

The copy that never got the fix is the one a live retrieve uses.
`matrixark_local_adapter_retrieve` -- the `retrieve` mixin the local adapter serves from -- imports
`cosine` from `matrixark_mcp_core` and calls it at twelve sites, scoring events, entities, segments,
summaries, resources and compressions, then feeds each result to `normalized_dense_score`, which
clamps into [0, 1]. An unnormalised dot of 32.0 arrives there as exactly 1.0, so every candidate
above the clamp pins to the endpoint together and their relative order is gone.

The three `matrixark_mcp_retrieve_*_scan` modules import it the same way and are checked below, but
they are reached only by tests -- nothing outside `tools/test_*` imports them. Naming those as "the
retrieval path" was the first answer here, and it was wrong: a call site is not a code path.

Vectors stored today are unit length, so the two answered the same and nothing had gone wrong. That
is what let one copy be fixed while the other was not.

The rule here is the strong form -- not "the two must agree" but "there must not be two". A
same-answer check would have passed for years on unit vectors, which is exactly how this arose.
"""
from __future__ import annotations

import ast
import importlib
import inspect
import subprocess
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
SELF = Path(__file__).name

#: The module that scores a live retrieve. `_LocalAdapterRetrieveMixin.retrieve` calls `cosine` at
#: twelve sites -- events, entities, segments, summaries, resources and compressions -- and takes
#: the result through `normalized_dense_score` into the dense half of the blend.
LIVE_CONSUMER = "matrixark_local_adapter_retrieve"

#: Reached only by tests. They are checked too, so that a second implementation cannot appear here
#: and be adopted later, but a pass on THESE is not evidence about a live retrieve -- which is the
#: mistake this list is written down to stop repeating. Nothing outside tools/test_* imports any of
#: them, and no non-Python file names them.
TEST_ONLY_SCANS = (
    "matrixark_mcp_retrieve_entity_scan",
    "matrixark_mcp_retrieve_summary_scan",
    "matrixark_mcp_retrieve_compression_scan",
)

HOME = "matrixark_mcp_scoring"


def _import(name: str):
    """Import a tools module whichever way the suite is being run.

    The suite runs both from tools/ (bare names) and from the repository root (as
    `tools.<module>`), and every module in the tree carries the same try/except for this reason.
    A guard that picks one path fails on the OTHER path, which reads as a defect in the code it
    is guarding rather than in how it was invoked.
    """
    try:
        return importlib.import_module("tools." + name)
    except ImportError:
        return importlib.import_module(name)


def _module_name(obj) -> str:
    """The defining module's name without its package prefix.

    The suite runs both from tools/ and as `tools.<module>`, so the same function reports
    `matrixark_mcp_scoring` in one and `tools.matrixark_mcp_scoring` in the other. Comparing whole
    dotted names makes this file fail on the import path rather than on what it is checking.
    """
    module = inspect.getmodule(obj)
    return getattr(module, "__name__", "").rsplit(".", 1)[-1]



def _definitions_of_cosine() -> list[str]:
    listed = subprocess.run(
        ["git", "ls-files", "tools/*.py"], cwd=REPO, capture_output=True, text=True).stdout.split()
    found = []
    for rel in listed:
        if Path(rel).name in (SELF,):
            continue
        try:
            tree = ast.parse((REPO / rel).read_text(encoding="utf-8", errors="replace"))
        except (SyntaxError, OSError):
            continue
        for node in tree.body:
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == "cosine":
                found.append(Path(rel).stem)
    return sorted(found)


class ThereIsOneCosineTest(unittest.TestCase):

    def test_only_one_module_defines_it(self) -> None:
        self.assertEqual(
            [HOME], _definitions_of_cosine(),
            "cosine is defined in more than one module again. The second copy will answer the same "
            "for the unit vectors stored today and differently for anything compacted, which is "
            "how the first divergence went unnoticed -- re-export it instead of reimplementing it")

    def test_the_live_retrieve_reaches_that_one(self) -> None:
        """The assertion that is about a running system rather than about a test fixture."""
        _import("matrixark_mcp_local_adapter")      # the mixin is reached through the adapter
        module = _import(LIVE_CONSUMER)
        reached = _module_name(module.cosine)
        self.assertEqual(
            HOME, reached,
            "%s scores a live retrieve with a cosine from %s rather than %s, at twelve call sites"
            % (LIVE_CONSUMER, reached, HOME))

    def test_the_test_only_scans_reach_it_too(self) -> None:
        for name in TEST_ONLY_SCANS:
            module = _import(name)
            reached = _module_name(module.cosine)
            self.assertEqual(
                HOME, reached,
                "%s scores candidates with a cosine from %s rather than %s"
                % (name, reached, HOME))

    def test_it_normalises_rather_than_returning_the_dot(self) -> None:
        """The discriminating case, and the control that shows why one alone proves nothing."""
        core = _import("matrixark_mcp_core")

        self.assertEqual(
            1.0, core.cosine([0.6, 0.8], [0.6, 0.8]),
            "unit vectors must be unaffected -- if this changes, the fix is not behaviour-"
            "preserving for what is stored today")

        self.assertAlmostEqual(
            0.974632, core.cosine([1.0, 2.0, 3.0], [4.0, 5.0, 6.0]), places=6,
            msg="cosine returned the bare dot product (32.0) for a non-unit pair. The unit-vector "
            "assertion above passes either way, which is why it cannot stand alone")

    def test_the_clamp_no_longer_swallows_the_ranking(self) -> None:
        """The consequence the scans actually suffer, asserted where they suffer it."""
        core = _import("matrixark_mcp_core")
        near = core.normalized_dense_score(core.cosine([1.0, 2.0, 3.0], [4.0, 5.0, 6.0]))
        far = core.normalized_dense_score(core.cosine([1.0, 2.0, 3.0], [6.0, 5.0, 4.0]))
        self.assertNotEqual(
            near, far,
            "two candidates at different angles score identically after the clamp, so their order "
            "is decided by whatever tie-break follows rather than by the vectors")
        self.assertLess(near, 1.0, "a non-unit pair still pins to the top of the clamp")
        self.assertGreater(near, far, "the closer pair must still rank higher")


if __name__ == "__main__":
    unittest.main()
