# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""int8 stays off on this path, for a ranking reason and a scorer reason.

The ranking reason is decisive and already established: over 500 candidate chunks with six CN/EN
queries, scored against the float ranking, int8 got top-1 right 1/6 and exact top-10 order 0/6,
mean overlap 4.8/10. A reconstruction cosine near 0.9999 is not evidence of safety -- the margins
between competing near-neighbours are smaller than that error.

A 2026-08-31 re-measurement (multilingual-e5-large @512, 2,753 chunks, 298 queries) found
reconstruction cosine min 0.999816 and recall@1 0.7617 -> 0.7584. That does not overturn it:
needle-match recall tolerates reordering among similarly relevant chunks; rank agreement does not.

The second reason is downstream. matrixark_mcp_scoring.cosine is a bare dot product over assumed-unit
inputs, and normalized_dense_score clamps (v + 1) / 2 into [0, 1]. At int8 magnitudes every
positive match saturates to 1.0 and every negative to 0.0, collapsing the dense term to a binary
signal so that 0.72 * dense + 0.28 * sparse is decided by the lexical term alone.

These tests exist so the default cannot be flipped on again without the scorer being fixed first:
the saturation test fails the moment someone enables int8 while cosine() still skips the norms.
"""
import importlib
import math
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from matrixark_mcp_scoring import cosine, normalized_dense_score


def _core(**env):
    """Re-import under a given environment: the flag is read at import time."""
    saved = {k: os.environ.get(k) for k in env}
    for k, v in env.items():
        if v is None:
            os.environ.pop(k, None)
        else:
            os.environ[k] = v
    for name in [m for m in list(sys.modules) if m.startswith("matrixark_mcp_core")]:
        del sys.modules[name]
    module = importlib.import_module("matrixark_mcp_core")
    for k, v in saved.items():
        if v is None:
            os.environ.pop(k, None)
        else:
            os.environ[k] = v
    return module


def _unit(seed, dims=512):
    values = [math.sin(seed * 0.7 + i * 0.013) for i in range(dims)]
    norm = math.sqrt(sum(v * v for v in values))
    return [v / norm for v in values]


class Int8StaysOffOnThisPath(unittest.TestCase):
    def test_int8_is_off_by_default_here(self):
        core = _core(MATRIXARK_EMBEDDING_VECTOR_INT8=None)
        self.assertFalse(
            core.EMBEDDING_VECTOR_INT8,
            "int8 saturates this path's dense score; it belongs on the Rust cosine path",
        )

    def test_the_scorer_skips_the_norms_which_is_why(self):
        # If this ever starts normalising, int8 becomes safe here and the default can move.
        big = [2.0, 0.0, 0.0]
        unit_vector = [1.0, 0.0, 0.0]
        self.assertEqual(
            2.0, cosine(unit_vector, big),
            "cosine() is a bare dot product; a true cosine would return 1.0 for these",
        )

    def test_quantised_vectors_saturate_the_dense_score(self):
        core = _core(MATRIXARK_EMBEDDING_VECTOR_INT8="1")
        query = _unit(11)
        dense = []
        for seed in range(1, 9):
            stored = core.compact_embedding_vector(_unit(seed))
            dense.append(normalized_dense_score(cosine(query, stored)))
        # Every value pinned to an endpoint: the dense term carries no gradation left to rank by.
        self.assertTrue(
            all(value in (0.0, 1.0) for value in dense),
            "expected saturation at the clamp endpoints, got %r" % dense,
        )
        self.assertIn(1.0, dense)
        self.assertIn(0.0, dense)

    def test_float_vectors_keep_their_gradation(self):
        # The contrast that makes the previous test meaningful rather than vacuous.
        query = _unit(11)
        dense = [normalized_dense_score(cosine(query, _unit(seed))) for seed in range(1, 9)]
        interior = [value for value in dense if 0.0 < value < 1.0]
        self.assertEqual(
            len(dense), len(interior),
            "unit vectors must land strictly inside the clamp, got %r" % dense,
        )

    def test_high_reconstruction_fidelity_is_not_evidence_of_safe_ranking(self):
        # The trap this pins: reconstruction cosine clears 0.999 comfortably while the measured
        # top-10 overlap against float ranking is 4.8/10. A passing fidelity number must never be
        # read as permission to enable int8 for retrieval.
        core = _core(MATRIXARK_EMBEDDING_VECTOR_INT8="1")
        for seed in range(1, 12):
            original = _unit(seed)
            quantised = core.compact_embedding_vector(original)
            dot = sum(a * b for a, b in zip(original, quantised))
            na = math.sqrt(sum(a * a for a in original))
            nb = math.sqrt(sum(b * b for b in quantised))
            self.assertGreaterEqual(
                dot / (na * nb), 0.999,
                "reconstruction cosine fell below the 0.999 floor at seed %d" % seed,
            )


if __name__ == "__main__":
    unittest.main()
