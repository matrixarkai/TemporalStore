# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Vector compaction has two implementations, and they must stay one behaviour.

compact_embedding_vector is 44% of record emission -- 1,690,624 element operations for a 1 MB
skill, one per dimension of every chunk vector. The arithmetic is trivial and the Python loop is
not, so it runs in numpy where numpy exists: emission 1,143ms -> 525ms per document, 2.18x, about
10 minutes saved per thousand documents.

numpy is not assumed. The loop remains for installs without it, which means two implementations of
one behaviour -- the failure mode being that they agree on ordinary vectors and diverge on the
awkward ones. These tests feed both paths the cases that separate rounding implementations: exact
.5 boundaries at each encoding's precision, the all-zero vector whose peak an int8 scale would
divide by, negative zero, and the empty vector.
"""
import importlib
import math
import os
import random
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def _core(encoding):
    os.environ["MATRIXARK_EMBEDDING_VECTOR_INT8"] = "1" if encoding == "int8" else "0"
    os.environ["MATRIXARK_EMBEDDING_VECTOR_SCALE"] = "100000" if encoding == "scale" else "0"
    for name in [m for m in list(sys.modules) if m.startswith("matrixark_mcp_core")]:
        del sys.modules[name]
    return importlib.import_module("matrixark_mcp_core")


def _both(core, vector):
    """The same vector through the numpy path and the fallback."""
    fast = core.compact_embedding_vector(vector)
    saved = core._NUMPY
    core._NUMPY = None
    try:
        slow = core.compact_embedding_vector(vector)
    finally:
        core._NUMPY = saved
    return fast, slow


def _unit(seed, dims=64):
    rng = random.Random(seed)
    values = [rng.gauss(0, 1) for _ in range(dims)]
    norm = math.sqrt(sum(v * v for v in values))
    return [v / norm for v in values]


AWKWARD = [
    [],
    [0.0] * 8,
    [0.0000005, -0.0000005, 0.0000015, -0.0000015],
    [0.000005, 0.000015, 0.000025, 0.000035],
    [1.0, -1.0, 0.5, -0.5],
    [1e-12, -1e-12],
    [-0.0],
]


class TheTwoPathsAreOneBehaviour(unittest.TestCase):
    def test_numpy_is_actually_in_use(self):
        # Without this the comparisons below would run the fallback against itself and pass
        # no matter what the numpy branch did.
        core = _core("float")
        self.assertIsNotNone(
            core._NUMPY,
            "numpy is absent, so these tests compare the fallback with itself and prove nothing")

    def test_paths_agree_on_ordinary_vectors(self):
        for encoding in ("float", "scale", "int8"):
            core = _core(encoding)
            for seed in range(40):
                fast, slow = _both(core, _unit(seed))
                self.assertEqual(fast, slow, "%s encoding diverged on seed %d" % (encoding, seed))

    def test_paths_agree_on_exact_half_boundaries_and_zeros(self):
        # Where two rounding implementations diverge if either does not round half to even.
        for encoding in ("float", "scale", "int8"):
            core = _core(encoding)
            for vector in AWKWARD:
                fast, slow = _both(core, vector)
                self.assertEqual(fast, slow,
                                 "%s encoding diverged on %r" % (encoding, vector[:4]))

    def test_an_all_zero_vector_does_not_divide_by_its_peak(self):
        core = _core("int8")
        fast, slow = _both(core, [0.0] * 8)
        self.assertEqual([0] * 8, fast)
        self.assertEqual([0] * 8, slow)

    def test_int8_stays_inside_the_range_on_both_paths(self):
        core = _core("int8")
        for seed in range(20):
            for out in _both(core, _unit(seed)):
                self.assertLessEqual(max(out), 127)
                self.assertGreaterEqual(min(out), -128)
                self.assertEqual(127, max(abs(v) for v in out),
                                 "the peak element must anchor the scale")

    def test_float_encoding_keeps_its_declared_precision(self):
        core = _core("float")
        for out in _both(core, _unit(3)):
            for value in out:
                self.assertEqual(value, round(value, core.EMBEDDING_VECTOR_DECIMALS))


if __name__ == "__main__":
    unittest.main()
