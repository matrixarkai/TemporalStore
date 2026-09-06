# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""The vector can be stored as packed f32 rather than a JSON array of floats.

A 384-dimension vector written as JSON costs 20.96 bytes per dimension -- the digits of a float are
most of a record once the encoder is a real model. Packed little-endian f32 in base64 is 5.33. The
comparison is made AFTER compression, because the log writes zlib blocks and a saving compression
already takes is not a saving:

    256 vectors of 384 dimensions, one block
                                plain        zlib-6   vs JSON compressed
    JSON array of floats    2,061,356       906,442          1.00x
    f32 base64                537,775       397,632          2.28x
    int16 + scale             275,631       202,646          4.47x
    int8 + scale              144,559       102,612          8.83x

**On this environment it is a wash -- 1.0%.** These vectors are 32 dimensions from a token hash, so
most elements are exactly 0.0 and JSON spends three characters on them; the win is a property of
production-width model vectors, not of this fixture. Said plainly here so nobody reads the 2.28x as
something this box demonstrated.

f32 and not int8. f32 is EXACT, so there is no recall question and no quality bar to clear. int8 is
worth 8.83x and is a separate decision: on 2,000 random unit vectors the reconstruction cosine
passes comfortably (min 0.999938) while recall@1 drops 0.0100 against a bar of 0.005 -- and random
vectors in 384 dimensions are near-orthogonal, the hardest possible case and not what an encoder
produces. That belongs on real vectors.

OFF by default. A reader from before this change finds no `vector` on a packed record and carries on
without one: lost recall, no error. Every other switch in this family degrades to re-deriving from
the log; this one degrades to a quietly worse answer, which is worth refusing to ship on.
"""
import base64
import json
import struct
import tempfile
import unittest
from pathlib import Path

from tools import matrixark_mcp_local_adapter as adapter_module
from tools.matrixark_mcp_local_adapter import (
    VECTOR_F32_KEY,
    decode_vector_f32,
    encode_vector_f32,
    expand_interned_records,
    pack_record_vectors,
    unpack_record_vectors,
    MatrixArkLocalAdapter,
    _LOCAL_READ_CACHE,
    _LOCAL_READ_CACHE_LOCK,
)


def _clear_process_read_cache() -> None:
    with _LOCAL_READ_CACHE_LOCK:
        _LOCAL_READ_CACHE.clear()


SCOPE = {"tenant_id": "acme", "user_id": "dana", "session_id": "skills"}


def _skill_text(index: int, sections: int = 40) -> str:
    out = ["# Runbook %d" % index, ""]
    for section in range(sections):
        out += ["## Step %d" % section, "",
                "Check the queue depth for case %d step %d, then drain the backlog." % (index, section),
                ""]
    return "\n".join(out)


class VectorCanBeStoredPackedTest(unittest.TestCase):
    def setUp(self) -> None:
        self._dir = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(self._dir.cleanup)
        self.store = Path(self._dir.name)
        self.log = self.store / "events.jsonl"
        _clear_process_read_cache()
        self.addCleanup(_clear_process_read_cache)
        self.addCleanup(setattr, adapter_module, "LOCAL_BINARY_VECTORS",
                        adapter_module.LOCAL_BINARY_VECTORS)

    def _ingest(self, documents: int = 4) -> list:
        for index in range(documents):
            adapter = MatrixArkLocalAdapter(self.log)
            adapter.ingest({"kind": "skill", "scope": SCOPE, "text": _skill_text(index),
                            "metadata": {"raw_uri": "file:///s/d-%d.md" % index,
                                         "title": "d-%d" % index}})
            adapter.close(timeout_s=3600)
        _clear_process_read_cache()
        return MatrixArkLocalAdapter(self.log).read_all()

    def _stored(self) -> list:
        return [json.loads(line) for line in adapter_module._iter_shard_lines(self.log)
                if line.strip()]

    def test_the_round_trip_is_exact(self):
        """f32 is chosen over int8 precisely because it is exact, so the test has to prove it."""
        values = [0.0, -0.5, 1.0, 0.123456789, -3.4028235e38, 1e-8]
        back = decode_vector_f32(encode_vector_f32(values))
        self.assertEqual(len(values), len(back))
        for original, restored in zip(values, back):
            self.assertEqual(struct.unpack("<f", struct.pack("<f", original))[0], restored,
                             "f32 storage is not returning the float32 value it was given")

    def test_a_packed_vector_is_smaller_per_dimension_at_model_width(self):
        """The claim is about a model's vector, so the test uses one rather than this box's hash."""
        vector = [((index * 37) % 1000) / 1000.0 - 0.5 for index in range(384)]
        as_json = json.dumps(vector, separators=(",", ":")).encode("utf-8")
        packed = encode_vector_f32(vector).encode("ascii")
        self.assertLess(len(packed), len(as_json) / 2,
                        "packed f32 is not materially smaller than the JSON digits at 384 dims")

    def test_the_log_holds_the_packed_form_and_the_view_is_unchanged(self):
        adapter_module.LOCAL_BINARY_VECTORS = False
        plain_view = self._ingest()
        plain_vectors = [r["vector"] for r in plain_view if isinstance(r.get("vector"), list)]
        self.assertTrue(plain_vectors, "no vectors were stored, so this proves nothing")
        self.assertTrue(any(isinstance(r.get("vector"), list) for r in self._stored()))

        self._dir.cleanup()
        self._dir = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(self._dir.cleanup)
        self.store = Path(self._dir.name)
        self.log = self.store / "events.jsonl"
        _clear_process_read_cache()

        adapter_module.LOCAL_BINARY_VECTORS = True
        packed_view = self._ingest()
        stored = self._stored()
        self.assertTrue(any(isinstance(r.get(VECTOR_F32_KEY), str) for r in stored),
                        "the log does not hold the packed form")
        self.assertFalse(any(isinstance(r.get("vector"), list) for r in stored),
                         "a JSON float array survived beside the packed form")

        packed_vectors = [r["vector"] for r in packed_view if isinstance(r.get("vector"), list)]
        self.assertEqual(len(plain_vectors), len(packed_vectors))
        for left, right in zip(plain_vectors, packed_vectors):
            self.assertEqual([struct.unpack("<f", struct.pack("<f", v))[0] for v in left], right,
                             "the served vector changed; this is a storage form, not a codec")

    def test_a_packed_log_reads_with_the_flag_off(self):
        """The flag chooses what to WRITE. It must never decide what can be read, or turning it off
        would make every store written with it on lose its vectors."""
        adapter_module.LOCAL_BINARY_VECTORS = True
        written = self._ingest()
        adapter_module.LOCAL_BINARY_VECTORS = False
        _clear_process_read_cache()
        self.assertEqual(written, MatrixArkLocalAdapter(self.log).read_all())

    def test_expansion_unpacks_even_when_nothing_is_interned(self):
        """expand_interned_records returns early when no record carries a token, and a packed vector
        has nothing to do with interning -- so the unpacking has to happen before that fast path."""
        vector = [0.25, -0.5, 0.75]
        record = {"record_type": "context_event", VECTOR_F32_KEY: encode_vector_f32(vector)}
        expanded = expand_interned_records([record])
        self.assertEqual(1, len(expanded))
        self.assertNotIn(VECTOR_F32_KEY, expanded[0])
        self.assertEqual(vector, expanded[0]["vector"])

    def test_a_damaged_vector_loses_the_vector_not_the_record(self):
        """A row without a vector already has a path -- it falls through to the lexical pass. A row
        that cannot be decoded at all would take its text and identity down with it."""
        record = {"record_type": "context_event", "text": "keep me",
                  VECTOR_F32_KEY: base64.b64encode(b"\x01\x02\x03").decode("ascii")}
        out = unpack_record_vectors([record])
        self.assertEqual(1, len(out))
        self.assertEqual("keep me", out[0]["text"])

    def test_packing_leaves_everything_that_is_not_a_float_list_alone(self):
        adapter_module.LOCAL_BINARY_VECTORS = True
        records = [
            {"record_type": "a", "vector": "already a string"},
            {"record_type": "b", "vector": []},
            {"record_type": "c", "vector": [True, False]},
            {"record_type": "d", "vector": ["x", "y"]},
            {"record_type": "e"},
        ]
        self.assertEqual(records, pack_record_vectors([dict(r) for r in records]))

    def test_the_flag_off_writes_the_json_array(self):
        adapter_module.LOCAL_BINARY_VECTORS = False
        records = [{"record_type": "a", "vector": [0.5, 0.25]}]
        self.assertEqual(records, pack_record_vectors([dict(r) for r in records]))


    def test_a_warm_read_and_a_cold_read_agree_on_the_vector(self):
        """The defect this had. Packing on the way to the LOG only left the cache and the snapshot
        holding the original float64s, so a warm read answered -0.408248 and a cold read that
        re-derived from the log answered -0.40824800729751587: the same store, two answers, decided
        by which path served it. Storing f32 has to mean holding f32."""
        adapter_module.LOCAL_BINARY_VECTORS = True
        warm = self._ingest()
        warm_vectors = [r["vector"] for r in warm if isinstance(r.get("vector"), list)]
        self.assertTrue(warm_vectors, "no vectors, so this compares nothing")

        # A cold reader with no snapshot at all: the answer can only come from the log.
        for path in list(self.store.iterdir()):
            if "read-cache" in path.name:
                path.unlink()
        _clear_process_read_cache()
        cold = MatrixArkLocalAdapter(self.log).read_all()
        cold_vectors = [r["vector"] for r in cold if isinstance(r.get("vector"), list)]

        self.assertEqual(warm_vectors, cold_vectors,
                         "the warm and cold paths disagree about the vector, to the bit")

    def test_both_append_paths_round_the_vector(self):
        """append and append_many build their sanitized list in separate comprehensions, and the
        first version of this rounded in one of them -- the other spells the same loop with a
        different variable name, so it was missed. Rounding lives in `_sanitize_jsonl_record`, which
        both run per record; this asserts both, so moving it back out fails here."""
        adapter_module.LOCAL_BINARY_VECTORS = True
        vector = [0.1, 0.2, 0.30000000000000004, -0.7]
        expected = [struct.unpack("<f", struct.pack("<f", v))[0] for v in vector]

        one = MatrixArkLocalAdapter(self.log)._sanitize_jsonl_record(
            {"record_type": "context_event", "event_id_hash": 1, "vector": list(vector)})
        self.assertEqual(expected, one["vector"], "the sanitize chokepoint did not round")

        adapter = MatrixArkLocalAdapter(self.log)
        adapter.append_many([{"record_type": "context_event", "event_id_hash": 2,
                              "vector": list(vector), "updated_at_ms": 1780000000002}])
        adapter.append({"record_type": "context_event", "event_id_hash": 3,
                        "vector": list(vector), "updated_at_ms": 1780000000003})
        _clear_process_read_cache()
        served = {r.get("event_id_hash"): r for r in MatrixArkLocalAdapter(self.log).read_all()
                  if isinstance(r.get("vector"), list)}
        for event_id in (2, 3):
            self.assertIn(event_id, served, "record %d did not come back with a vector" % event_id)
            self.assertEqual(expected, served[event_id]["vector"],
                             "append path for record %d did not round the vector" % event_id)


if __name__ == "__main__":
    unittest.main()
