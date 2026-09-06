# SPDX-License-Identifier: Apache-2.0
"""There is one cosine, and the retrieval path reaches it.

`cosine` was implemented twice. `matrixark_mcp_scoring.cosine` divides by both norms and carries a
docstring explaining why: the bare dot product is the same number as a cosine while both sides are
unit vectors and a different one as soon as either is not, and every vector-compaction option
produces a non-unit vector. `matrixark_mcp_core.cosine` was the bare dot product, and never got
that fix.

The copy that never got the fix was the one the retrieval path used.
`matrixark_mcp_retrieve_entity_scan`, `matrixark_mcp_retrieve_summary_scan` and
`matrixark_mcp_retrieve_compression_scan` each import `cosine` from `matrixark_mcp_core` and feed
the result to `normalized_dense_score`, which clamps into [0, 1]. An unnormalised dot of 32.0
arrives there as exactly 1.0, so every candidate above the clamp pins to the endpoint together and
their relative order is gone.

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

#: Every module that reads `cosine` to score a retrieval candidate.
SCAN_MODULES = (
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

    def test_every_retrieval_scan_reaches_that_one(self) -> None:
        for name in SCAN_MODULES:
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
