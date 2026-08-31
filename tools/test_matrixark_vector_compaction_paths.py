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

try:  # the fallback path is supported, so numpy is optional here too
    import numpy  # noqa: F401
    _HAVE_NUMPY = True
except ImportError:
    _HAVE_NUMPY = False

_NEEDS_BOTH = unittest.skipUnless(
    _HAVE_NUMPY, "numpy absent: only one implementation exists, nothing to compare")


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
    def test_numpy_is_in_use_where_it_is_installed(self):
        # The comparisons below are only meaningful when both implementations exist. Where numpy
        # is absent there is one implementation and nothing to compare, which is a supported
        # configuration -- not a failure.
        core = _core("float")
        if _HAVE_NUMPY:
            self.assertIsNotNone(
                core._NUMPY,
                "numpy is installed but the module did not pick it up, so the equivalence "
                "tests would compare the fallback with itself")
        else:
            self.assertIsNone(core._NUMPY)

    @_NEEDS_BOTH
    def test_paths_agree_on_ordinary_vectors(self):
        for encoding in ("float", "scale", "int8"):
            core = _core(encoding)
            for seed in range(40):
                fast, slow = _both(core, _unit(seed))
                self.assertEqual(fast, slow, "%s encoding diverged on seed %d" % (encoding, seed))

    @_NEEDS_BOTH
    def test_paths_agree_on_exact_half_boundaries_and_zeros(self):
        # Where two rounding implementations diverge if either does not round half to even.
        for encoding in ("float", "scale", "int8"):
            core = _core(encoding)
            for vector in AWKWARD:
                fast, slow = _both(core, vector)
                self.assertEqual(fast, slow,
                                 "%s encoding diverged on %r" % (encoding, vector[:4]))

    @_NEEDS_BOTH
    def test_an_all_zero_vector_does_not_divide_by_its_peak(self):
        core = _core("int8")
        fast, slow = _both(core, [0.0] * 8)
        self.assertEqual([0] * 8, fast)
        self.assertEqual([0] * 8, slow)

    @_NEEDS_BOTH
    def test_int8_stays_inside_the_range_on_both_paths(self):
        core = _core("int8")
        for seed in range(20):
            for out in _both(core, _unit(seed)):
                self.assertLessEqual(max(out), 127)
                self.assertGreaterEqual(min(out), -127)

    def test_the_scale_does_not_depend_on_the_vector(self):
        """The property the whole encoding depends on, tested where the two rules DIVERGE.

        Dividing each vector by its own peak is what made int8 reorder: two stored vectors scaled
        by different factors can swap places against one query, which no amount of precision
        repairs. The clean discriminator is doubling a vector. Under a per-vector peak, a vector
        and its double normalise to the SAME output, because the peak divides the magnitude away.
        Under a width-derived scale the output doubles with the input.

        Recovering the factor by dividing output by input does not work: quantisation rounding on
        small elements swamps it, which is what made an earlier version of this test fail against
        correct code.
        """
        core = _core("int8")
        vector = _unit(3)
        once = core.compact_embedding_vector(vector)
        twice = core.compact_embedding_vector([v * 2 for v in vector])
        self.assertNotEqual(
            once, twice,
            "a vector and its double produced identical output, which is the per-vector peak "
            "behaviour: the magnitude was normalised away and two vectors can be scaled "
            "differently")
        # And it really is doubling, not merely differing.
        big = [v for v in zip(once, twice) if abs(v[0]) > 8]
        self.assertTrue(big, "no elements large enough to compare a ratio against")
        for small, large in big:
            self.assertAlmostEqual(2.0, large / small, delta=0.25)

    def test_the_scale_follows_the_width(self):
        core = _core("int8")
        self.assertAlmostEqual(127.0 * (64 ** 0.5) / 8.0, core._int8_scale(64), places=6)
        self.assertAlmostEqual(127.0 * (512 ** 0.5) / 8.0, core._int8_scale(512), places=6)
        self.assertGreater(core._int8_scale(512), core._int8_scale(64),
                           "a wider vector has smaller elements and needs a larger factor")

    @_NEEDS_BOTH
    def test_float_encoding_keeps_its_declared_precision(self):
        core = _core("float")
        for out in _both(core, _unit(3)):
            for value in out:
                self.assertEqual(value, round(value, core.EMBEDDING_VECTOR_DECIMALS))


if __name__ == "__main__":
    unittest.main()
