# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""The stored vector has two containers and must read as one thing.

JSON spends a character per digit: a 512-dim vector at scale=1e4 is 2,745 bytes of text to carry
512 values that fit in 1,024 bytes of int16. base64 of those bytes is 1,368 -- the same integers,
half the size. Vectors are about 59% of a skill ingest, so the container is the largest remaining
lever that costs no quality at all.

The danger is not corruption, it is silence. A reader that expects a list and receives a string
does not raise: it treats the vector as absent, the node stops being scored, and retrieval quietly
returns less. So every test here is about the two forms being INTERCHANGEABLE, and the decoder is
exercised on the shapes a wrong reader would produce.
"""
import base64
import importlib
import os
import struct
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def _core(enabled):
    os.environ["MATRIXARK_EMBEDDING_VECTOR_BASE64"] = "1" if enabled else "0"
    for name in [m for m in list(sys.modules) if m.startswith("matrixark_mcp_core")]:
        del sys.modules[name]
    return importlib.import_module("matrixark_mcp_core")


VALUES = [0, 1, -1, 127, -128, 3036, -1649, 10000, -10000, 32767, -32768]


class TheTwoContainersAreOneVector(unittest.TestCase):
    def test_the_flag_is_off_by_default(self):
        os.environ.pop("MATRIXARK_EMBEDDING_VECTOR_BASE64", None)
        for name in [m for m in list(sys.modules) if m.startswith("matrixark_mcp_core")]:
            del sys.modules[name]
        core = importlib.import_module("matrixark_mcp_core")
        self.assertFalse(
            core.EMBEDDING_VECTOR_BASE64,
            "every reader must go through decode_stored_vector before this is enabled")

    def test_round_trip_is_exact(self):
        core = _core(True)
        encoded = core.encode_stored_vector(VALUES)
        self.assertIsInstance(encoded, str, "with the flag on the stored form is a string")
        self.assertEqual(VALUES, core.decode_stored_vector(encoded))

    def test_the_encoded_form_is_smaller(self):
        # The entire reason for the container. If it is not smaller it has no purpose.
        import json
        core = _core(True)
        wide = [(i % 6000) - 3000 for i in range(512)]
        as_json = len(json.dumps(wide).encode())
        as_b64 = len(core.encode_stored_vector(wide).encode())
        self.assertLess(as_b64, as_json)
        self.assertLess(as_b64 / as_json, 0.65,
                        "expected roughly half: %d vs %d" % (as_b64, as_json))

    def test_a_list_still_reads_when_the_flag_is_on(self):
        # A store written before this existed holds lists. Turning the flag on must not blind
        # the reader to them.
        core = _core(True)
        self.assertEqual([1, -2, 3], core.decode_stored_vector([1, -2, 3]))

    def test_an_encoded_vector_still_reads_when_the_flag_is_off(self):
        # And the reverse: disabling the flag must not blind the reader to what it already wrote.
        written = _core(True).encode_stored_vector(VALUES)
        self.assertEqual(VALUES, _core(False).decode_stored_vector(written))

    def test_the_flag_only_changes_the_container(self):
        on, off = _core(True), _core(False)
        self.assertEqual(off.encode_stored_vector(VALUES),
                         on.decode_stored_vector(on.encode_stored_vector(VALUES)))

    def test_record_vector_reads_either_form(self):
        core = _core(True)
        self.assertEqual(VALUES, core.record_vector({"vector": VALUES}))
        self.assertEqual(VALUES, core.record_vector({"vector": core.encode_stored_vector(VALUES)}))
        self.assertEqual([], core.record_vector({}))
        self.assertEqual([], core.record_vector(None))

    def test_an_untagged_string_is_not_mistaken_for_a_vector(self):
        # Without the tag, any string in the field would decode to noise.
        core = _core(True)
        self.assertEqual([], core.decode_stored_vector("hello"))
        self.assertEqual([], core.decode_stored_vector(base64.b64encode(b"hello").decode()))

    def test_values_outside_int16_fall_back_to_the_list(self):
        # scale=1e4 on a unit vector caps at 10,000, but a larger scale would overflow. Losing
        # precision silently would be worse than storing more bytes.
        core = _core(True)
        too_big = [40000, -40000]
        self.assertEqual(too_big, core.encode_stored_vector(too_big))
        self.assertEqual(too_big, core.decode_stored_vector(core.encode_stored_vector(too_big)))

    def test_an_empty_vector_stays_empty(self):
        core = _core(True)
        self.assertEqual([], core.encode_stored_vector([]))
        self.assertEqual([], core.decode_stored_vector([]))

    def test_the_real_scale_fits_with_headroom(self):
        # At scale=1e4 the largest value a UNIT vector can produce is all its mass on one axis.
        core = _core(True)
        worst = [10000, -10000]
        self.assertEqual(worst, core.decode_stored_vector(core.encode_stored_vector(worst)))
        self.assertGreater(32767 / 10000, 3.0, "int16 headroom over the worst case")


if __name__ == "__main__":
    unittest.main()
