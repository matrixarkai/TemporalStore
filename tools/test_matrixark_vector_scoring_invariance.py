# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A rescaled vector must score exactly where the float it replaced scored.

Every vector-compaction option produces a NON-unit vector: int8 divides each vector by its own
peak, an integer scale multiplies uniformly. `cosine()` used to return a bare dot product over
assumed-unit inputs, and `normalized_dense_score` clamps (v + 1) / 2 into [0, 1]. Together those
two facts meant a compacted vector could not be scored correctly:

  - a uniform scale cannot reorder anything, but every score landed outside [-1, 1] and pinned to
    a clamp endpoint, so the dense term became a binary signal and 0.72*dense + 0.28*sparse was
    decided by the lexical term alone;
  - int8's per-vector peak is a NON-uniform rescale, so under an unnormalised dot the relative
    order of two stored vectors against one query could genuinely change.

Dividing by both norms removes both failures, and is a no-op for the vectors stored today because
a unit vector's dot already IS its cosine. These tests pin that property -- not the encoding
defaults, which are a separate decision -- so the scorer cannot quietly regress to a bare dot.
"""
import math
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from matrixark_mcp_scoring import cosine, normalized_dense_score


def _unit(seed, dims=128):
    values = [math.sin(seed * 0.7 + i * 0.013) for i in range(dims)]
    norm = math.sqrt(sum(v * v for v in values))
    return [v / norm for v in values]


def _int8(vector):
    peak = max((abs(v) for v in vector), default=0.0) or 1.0
    return [int(round(v / peak * 127)) for v in vector]


def _scaled(vector, factor=100000):
    return [int(round(v * factor)) for v in vector]


class ARescaledVectorScoresLikeItsFloat(unittest.TestCase):
    def test_the_scorer_normalises_both_sides(self):
        # The regression guard: a bare dot product returns 2.0 here, a cosine returns 1.0.
        self.assertEqual(1.0, cosine([1.0, 0.0, 0.0], [2.0, 0.0, 0.0]))
        self.assertEqual(1.0, cosine([3.0, 0.0, 0.0], [0.5, 0.0, 0.0]))

    def test_a_uniform_scale_does_not_change_the_score(self):
        query = _unit(11)
        for seed in range(1, 12):
            stored = _unit(seed)
            self.assertAlmostEqual(
                cosine(query, stored), cosine(query, _scaled(stored)), places=4,
                msg="scaling moved the score at seed %d" % seed)

    def test_int8_does_not_change_the_score(self):
        query = _unit(11)
        for seed in range(1, 12):
            stored = _unit(seed)
            self.assertAlmostEqual(
                cosine(query, stored), cosine(query, _int8(stored)), places=2,
                msg="int8 moved the score at seed %d" % seed)

    def test_compacted_vectors_no_longer_saturate_the_clamp(self):
        # Previously every positive match pinned to 1.0 and every negative to 0.0, leaving the
        # dense term with no gradation to rank by.
        query = _unit(11)
        for encode in (_scaled, _int8):
            dense = [normalized_dense_score(cosine(query, encode(_unit(s))))
                     for s in range(1, 9)]
            interior = [d for d in dense if 0.0 < d < 1.0]
            self.assertEqual(
                len(dense), len(interior),
                "scores pinned to the clamp endpoints: %r" % dense)

    def test_ranking_is_preserved_under_both_encodings(self):
        # Ordering is what retrieval actually consumes; equal scores would be worthless if the
        # order still moved.
        query = _unit(11)
        stored = [_unit(1 + i * 0.05) for i in range(40)]
        base = sorted(range(len(stored)), key=lambda i: -cosine(query, stored[i]))
        for name, encode in (("scale", _scaled), ("int8", _int8)):
            got = sorted(range(len(stored)),
                         key=lambda i: -cosine(query, encode(stored[i])))
            self.assertEqual(base[:10], got[:10], "%s reordered the top 10" % name)

    def test_a_zero_or_ragged_vector_scores_zero_rather_than_dividing(self):
        self.assertEqual(0.0, cosine([0.0, 0.0], [1.0, 0.0]))
        self.assertEqual(0.0, cosine([1.0, 0.0], []))
        self.assertEqual(0.0, cosine([1.0, 0.0], [1.0, 0.0, 0.0]))


if __name__ == "__main__":
    unittest.main()
