# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""The read snapshot can be stored as a compressed binary container.

The snapshot is the largest artifact this module writes and, after interning, the most repetitive.
Measured on three 1.00 MB skills: 12.97 MB of JSON becomes 0.74 MB, 1,909 B a record becomes 109 --
17.55x, for about 32 ms of decode.

zlib rather than zstd: `dependencies = []` in pyproject is deliberate, and zstd's 27.53x is better
but not enough to earn the project its first runtime dependency. The container carries a codec byte
so another encoding can be added without a second format, which is the shape the engine's served
index already uses.

The container is written under its OWN filename. `_load_durable_read_cache` opens the snapshot with
`encoding="utf-8"` and catches `(FileNotFoundError, json.JSONDecodeError, OSError)`; compressed
bytes raise `UnicodeDecodeError`, which is a ValueError but NOT a JSONDecodeError, so a reader from
before this change would crash on them. Under its own name it finds no snapshot and re-derives from
the log -- something it already does.

It is OFF by default. That is a separate decision: about twenty tests across eight files name the
JSON snapshot to mean "the snapshot", and pointing each at `_durable_read_cache_snapshot_path()`
deserves its own change.
"""
import tempfile
import unittest
import zlib
from pathlib import Path

from tools import matrixark_mcp_local_adapter as adapter_module
from tools.matrixark_mcp_local_adapter import (
    _SNAPSHOT_CODEC_BLOCKS,
    _SNAPSHOT_CODEC_ZLIB,
    _DELTA_BLOCK_CODEC_ZLIB,
    _DELTA_BLOCK_CODEC_ZLIB_ARRAY,
    _DELTA_BLOCK_HEADER_BYTES,
    _decode_blocked_snapshot,
    _encode_delta_block,
    _encode_blocked_snapshot,
    _iter_delta_blocks,
    _SNAPSHOT_CONTAINER_MAGIC,
    _decode_snapshot_bytes,
    MatrixArkLocalAdapter,
    _LOCAL_READ_CACHE,
    _LOCAL_READ_CACHE_LOCK,
)

SCOPE = {"tenant_id": "acme", "user_id": "dana", "session_id": "skills"}


def _clear_process_read_cache() -> None:
    with _LOCAL_READ_CACHE_LOCK:
        _LOCAL_READ_CACHE.clear()


def _skill_text(index: int, sections: int = 60) -> str:
    out = ["# Runbook %d" % index, ""]
    for section in range(sections):
        out += ["## Step %d" % section, "",
                "Check the queue depth for case %d step %d, then drain the backlog." % (index, section),
                ""]
    return "\n".join(out)


class SnapshotIsACompressedContainerTest(unittest.TestCase):
    def setUp(self) -> None:
        self._dir = tempfile.TemporaryDirectory()
        self.addCleanup(self._dir.cleanup)
        self.store = Path(self._dir.name)
        self.log = self.store / "events.jsonl"
        _clear_process_read_cache()
        self.addCleanup(_clear_process_read_cache)
        # The flag is a module global read at write time, so it is set here and RESTORED -- a test
        # that leaves process-global state behind changes the tests that follow it.
        self._previous = adapter_module.LOCAL_DURABLE_READ_CACHE_COMPRESS
        self.addCleanup(
            setattr, adapter_module, "LOCAL_DURABLE_READ_CACHE_COMPRESS", self._previous)
        self.addCleanup(setattr, adapter_module, "LOCAL_DURABLE_READ_CACHE_BLOCK_RECORDS",
                        adapter_module.LOCAL_DURABLE_READ_CACHE_BLOCK_RECORDS)

    def _ingest(self, documents: int = 2) -> list:
        for index in range(documents):
            adapter = MatrixArkLocalAdapter(self.log)
            adapter.ingest({
                "kind": "skill", "scope": SCOPE, "text": _skill_text(index),
                "metadata": {"raw_uri": "file:///s/doc-%d.md" % index, "title": "doc-%d" % index},
            })
            adapter.close(timeout_s=3600)
        _clear_process_read_cache()
        return MatrixArkLocalAdapter(self.log).read_all()

    def _paths(self):
        adapter = MatrixArkLocalAdapter(self.log)
        return adapter._durable_read_cache_path(), adapter._durable_read_cache_binary_path()

    def test_with_the_container_on_the_snapshot_is_binary_and_smaller(self):
        adapter_module.LOCAL_DURABLE_READ_CACHE_COMPRESS = False
        plain = self._ingest()
        json_path, binary_path = self._paths()
        self.assertTrue(json_path.exists(), "the JSON form was not written with the flag off")
        plain_bytes = json_path.stat().st_size

        # Same corpus again, with the container on.
        self._dir.cleanup()
        self._dir = tempfile.TemporaryDirectory()
        self.addCleanup(self._dir.cleanup)     # the setUp cleanup registered the FIRST one
        self.store = Path(self._dir.name)
        self.log = self.store / "events.jsonl"
        _clear_process_read_cache()
        adapter_module.LOCAL_DURABLE_READ_CACHE_COMPRESS = True
        packed = self._ingest()
        json_path, binary_path = self._paths()

        self.assertTrue(binary_path.exists(), "the container was not written")
        self.assertFalse(json_path.exists(),
                         "both forms are present; a reader could serve the older one")
        self.assertLess(binary_path.stat().st_size, plain_bytes / 2,
                        "the container is not materially smaller, so it is not earning its format")
        self.assertEqual(len(plain), len(packed),
                         "the two encodings disagree about how many records there are")

    def test_the_container_says_what_it_is(self):
        adapter_module.LOCAL_DURABLE_READ_CACHE_COMPRESS = True
        self._ingest()
        _, binary_path = self._paths()
        raw = binary_path.read_bytes()
        self.assertTrue(raw.startswith(_SNAPSHOT_CONTAINER_MAGIC),
                        "the container does not carry its magic, so a reader cannot recognise it")
        codec = raw[len(_SNAPSHOT_CONTAINER_MAGIC):len(_SNAPSHOT_CONTAINER_MAGIC) + 1]
        self.assertEqual(_SNAPSHOT_CODEC_BLOCKS, codec,
                         "the base is written in blocks, so it must say so")
        # And the payload really is blocks, whose FIRST one carries the header.
        body = raw[len(_SNAPSHOT_CONTAINER_MAGIC) + 1:]
        blocks = list(_iter_delta_blocks(body))
        self.assertTrue(blocks, "the container carries no blocks")
        self.assertIn("schema_version", blocks[0][0])

    def test_a_cold_read_is_served_from_the_container(self):
        adapter_module.LOCAL_DURABLE_READ_CACHE_COMPRESS = True
        written = self._ingest()
        _clear_process_read_cache()
        reader = MatrixArkLocalAdapter(self.log)
        served = reader.read_all()
        self.assertEqual("durable", getattr(reader, "_read_cache_source", "?"),
                         "the container did not serve the read, so this proves nothing about it")
        self.assertEqual(written, served)

    def test_a_json_snapshot_already_on_disk_still_loads(self):
        """No migration: a store written before the container keeps loading after it is enabled."""
        adapter_module.LOCAL_DURABLE_READ_CACHE_COMPRESS = False
        written = self._ingest()
        json_path, binary_path = self._paths()
        self.assertTrue(json_path.exists())
        self.assertFalse(binary_path.exists())

        adapter_module.LOCAL_DURABLE_READ_CACHE_COMPRESS = True
        _clear_process_read_cache()
        reader = MatrixArkLocalAdapter(self.log)
        served = reader.read_all()
        self.assertEqual("durable", getattr(reader, "_read_cache_source", "?"),
                         "the existing JSON snapshot was not used, so a store would re-derive")
        self.assertEqual(written, served)

    def test_the_decoder_takes_either_form(self):
        payload = {"schema_version": 2, "records": [{"record_type": "x"}]}
        import json as _json
        plain = _json.dumps(payload, separators=(",", ":")).encode("utf-8")
        packed = (_SNAPSHOT_CONTAINER_MAGIC + _SNAPSHOT_CODEC_ZLIB + zlib.compress(plain, 6))
        self.assertEqual(payload, _decode_snapshot_bytes(plain))
        self.assertEqual(payload, _decode_snapshot_bytes(packed))

    def test_an_unknown_codec_is_refused_rather_than_guessed(self):
        packed = _SNAPSHOT_CONTAINER_MAGIC + b"\x7f" + b"whatever"
        with self.assertRaises(ValueError):
            _decode_snapshot_bytes(packed)


    def test_the_base_is_split_rather_than_written_as_one_document(self):
        """This is what bounds the cold read's transient.

        The whole-document container decodes with `zlib.decompress` into one bytes object, so the
        peak tracks the UNCOMPRESSED payload almost 1:1 -- measured at 0.83x of it on 40 documents
        and 0.98x on 80, which projects to a ~4.5 GB transient at a thousand 1 MB skills against a
        0.26 GB file. One block at a time is the fix, and a snapshot written as a single block would
        pass every other test here while paying the whole cost.

        Asserted on the encoder rather than on a store, because how many records an ingest leaves in
        the BASE (as opposed to the tail beside it) is not this format's business -- a test that
        depended on it would be testing the fold schedule.
        """
        adapter_module.LOCAL_DURABLE_READ_CACHE_BLOCK_RECORDS = 4
        records = [{"record_type": "context_event", "event_id_hash": index} for index in range(9)]
        payload = {"schema_version": 3, "record_count": 9, "records": records}
        body = _encode_blocked_snapshot(payload)[len(_SNAPSHOT_CONTAINER_MAGIC) + 1:]
        blocks = list(_iter_delta_blocks(body))

        self.assertIn("schema_version", blocks[0][0], "the first block is not the header")
        self.assertEqual([4, 4, 1], [len(block) for block in blocks[1:]],
                         "the records were not split into blocks of the size asked for")
        self.assertEqual(payload, _decode_snapshot_bytes(_encode_blocked_snapshot(payload)))

    def test_a_whole_document_container_still_loads(self):
        """No migration: a store written before the blocks keeps loading."""
        import json as _json

        payload = {"schema_version": 2, "records": [{"record_type": "x"}]}
        plain = _json.dumps(payload, separators=(",", ":")).encode("utf-8")
        whole = _SNAPSHOT_CONTAINER_MAGIC + _SNAPSHOT_CODEC_ZLIB + zlib.compress(plain, 6)
        self.assertEqual(payload, _decode_snapshot_bytes(whole))

    def test_a_blocked_snapshot_with_no_header_is_refused(self):
        """The header block is the contract; without it there is nothing to check a count against."""
        with self.assertRaises(ValueError):
            _decode_blocked_snapshot(b"")


    def test_a_block_holds_an_array_rather_than_a_document_per_line(self):
        """The framing was never the cost -- the per-record parse was.

        Newline-delimited records mean one json.loads call PER RECORD. On 60,000 records the line
        form decodes in 621 ms, the array form in 318, and a single whole document in 442, at the
        same size, because the compressor sees the same text either way. A block that reverted to
        lines would pass every other test here and cost 40% of the decode.
        """
        import json as _json

        block = _encode_delta_block([{"record_type": "a"}, {"record_type": "b"}])
        self.assertEqual(_DELTA_BLOCK_CODEC_ZLIB_ARRAY, block[:1],
                         "the block does not declare the array payload")
        body = zlib.decompress(block[_DELTA_BLOCK_HEADER_BYTES:])
        self.assertIsInstance(_json.loads(body), list,
                              "the payload is not a single JSON array")
        self.assertEqual([{"record_type": "a"}, {"record_type": "b"}],
                         list(_iter_delta_blocks(block))[0])

    def test_a_block_written_as_lines_still_reads(self):
        """A tail written by the build that first shipped blocks carries the line codec.

        Dropping it would make those tails undecodable rather than merely slower -- and a tail that
        cannot be decoded is a store that re-derives from the log on every read.
        """
        import json as _json

        payload = zlib.compress(
            b"\n".join(_json.dumps(record, separators=(",", ":")).encode("utf-8")
                         for record in ({"record_type": "a"}, {"record_type": "b"})), 6)
        block = _DELTA_BLOCK_CODEC_ZLIB + len(payload).to_bytes(4, "big") + payload
        self.assertEqual([{"record_type": "a"}, {"record_type": "b"}],
                         list(_iter_delta_blocks(block))[0])


    # -- the incremental half ---------------------------------------------------------------

    def test_the_tail_is_block_framed_and_much_smaller(self):
        """The tail is where the snapshot's bytes are once the base is compressed.

        With the base 17.55x smaller the plain tail became 85-99.9% of the snapshot, so compressing
        it is not a refinement -- it is most of what is left. One block per appended batch reaches
        15.6x where per-record framing reaches 3.3x.
        """
        adapter_module.LOCAL_DURABLE_READ_CACHE_COMPRESS = False
        self._ingest(3)
        plain = MatrixArkLocalAdapter(self.log)._durable_read_cache_delta_path()
        plain_bytes = plain.stat().st_size if plain.exists() else 0
        self.assertGreater(plain_bytes, 0, "no plain tail was written, so there is nothing to beat")

        self._fresh_store()
        adapter_module.LOCAL_DURABLE_READ_CACHE_COMPRESS = True
        self._ingest(3)
        adapter = MatrixArkLocalAdapter(self.log)
        packed = adapter._durable_read_cache_delta_binary_path()
        self.assertTrue(packed.exists(), "no block-framed tail was written")
        self.assertFalse(adapter._durable_read_cache_delta_path().exists(),
                         "both tails are present; a reader could pick up the stale one")
        self.assertLess(packed.stat().st_size, plain_bytes / 2,
                        "the block-framed tail is not materially smaller than the plain one")

    def test_a_cold_read_stitches_the_block_framed_tail(self):
        adapter_module.LOCAL_DURABLE_READ_CACHE_COMPRESS = True
        written = self._ingest(3)
        adapter = MatrixArkLocalAdapter(self.log)
        self.assertTrue(adapter._durable_read_cache_delta_binary_path().exists(),
                        "no tail was written, so this does not test stitching")
        _clear_process_read_cache()
        reader = MatrixArkLocalAdapter(self.log)
        served = reader.read_all()
        self.assertEqual("durable", getattr(reader, "_read_cache_source", "?"))
        self.assertEqual(written, served)

    def test_a_plain_tail_already_on_disk_still_loads(self):
        """A store written before the block format keeps loading, like the base's JSON form."""
        adapter_module.LOCAL_DURABLE_READ_CACHE_COMPRESS = False
        written = self._ingest(3)
        adapter = MatrixArkLocalAdapter(self.log)
        self.assertTrue(adapter._durable_read_cache_delta_path().exists())

        adapter_module.LOCAL_DURABLE_READ_CACHE_COMPRESS = True
        _clear_process_read_cache()
        reader = MatrixArkLocalAdapter(self.log)
        self.assertEqual(written, reader.read_all())

    def test_a_torn_final_block_is_dropped_not_raised(self):
        """A half-written block must not make the snapshot unreadable.

        The tail is derived state and the head says how many records it should hold, so a short read
        is caught by the count check and the caller re-derives from the log. Raising here would turn
        a torn append into an unreadable store when re-deriving is the answer.
        """
        from tools.matrixark_mcp_local_adapter import _decode_delta_blocks

        adapter_module.LOCAL_DURABLE_READ_CACHE_COMPRESS = True
        self._ingest(3)
        packed = MatrixArkLocalAdapter(self.log)._durable_read_cache_delta_binary_path()
        raw = packed.read_bytes()
        whole = len(_decode_delta_blocks(raw))
        self.assertGreater(whole, 0, "no records decoded, so truncation proves nothing")

        torn = _decode_delta_blocks(raw[: len(raw) - 10])
        self.assertLessEqual(len(torn), whole)

    def test_a_tail_in_the_other_form_is_refused_rather_than_joined(self):
        """Turning the container on over a store that already has a plain tail must not braid them.

        Refusing raises ValueError, which both append paths already catch and answer by falling
        through to a full rewrite -- and a rewrite clears both forms, leaving exactly one tail.
        """
        adapter_module.LOCAL_DURABLE_READ_CACHE_COMPRESS = True
        self._ingest(2)
        adapter = MatrixArkLocalAdapter(self.log)
        adapter._durable_read_cache_delta_path().write_text("{}", encoding="utf-8")
        with self.assertRaises(ValueError):
            adapter._append_tail_records([{"record_type": "context_event", "event_id_hash": 1}])

    def _fresh_store(self) -> None:
        self._dir.cleanup()
        self._dir = tempfile.TemporaryDirectory()
        self.addCleanup(self._dir.cleanup)
        self.store = Path(self._dir.name)
        self.log = self.store / "events.jsonl"
        _clear_process_read_cache()

if __name__ == "__main__":
    unittest.main()
