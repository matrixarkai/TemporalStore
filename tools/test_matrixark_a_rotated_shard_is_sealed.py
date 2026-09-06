# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A rotated shard is stored compressed; the active shard stays plain text.

Once the read snapshot became a container the event log was 94.3% of everything this module writes,
and at the default retention the ROTATED shards are 74.9% of the log -- 11.91x compressible. That is
where the durable bytes are.

What makes it safe is what a rotated shard is: sealed at rotation, never appended to again, reached
only through `_retained_jsonl_paths()`. The active shard -- the one appends land in, the one a
person greps -- is untouched, so nothing about appending or recovering changes.

The rename that rotates a shard stays the commit point. Sealing is a follow-up (temp write, fsync,
atomic replace), so a crash anywhere in it leaves a plain shard, which every reader still accepts.
"""
import tempfile
import unittest
import zlib
from pathlib import Path

from tools import matrixark_mcp_local_adapter as adapter_module
from tools.matrixark_mcp_local_adapter import (
    _SHARD_CODEC_ZLIB,
    _SHARD_CONTAINER_MAGIC,
    _iter_shard_lines,
    MatrixArkLocalAdapter,
    _LOCAL_READ_CACHE,
    _LOCAL_READ_CACHE_LOCK,
)


def _clear_process_read_cache() -> None:
    with _LOCAL_READ_CACHE_LOCK:
        _LOCAL_READ_CACHE.clear()


def _records(start: int, count: int) -> list[dict]:
    return [
        {
            "record_type": "context_event",
            "event_id_hash": index,
            "text": "event %d " % index + "x" * 400,
            "updated_at_ms": 1780000000000 + index,
        }
        for index in range(start, start + count)
    ]


class SealedShardTest(unittest.TestCase):
    def setUp(self) -> None:
        self._dir = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(self._dir.cleanup)
        self.log = Path(self._dir.name) / "events.jsonl"
        _clear_process_read_cache()
        self.addCleanup(_clear_process_read_cache)
        # Module globals read at call time, so they are set here and RESTORED -- a test that leaves
        # process-global state behind changes the tests after it.
        for name in ("LOCAL_JSONL_COMPRESS_SEALED", "LOCAL_JSONL_MAX_BYTES",
                     "LOCAL_JSONL_BLOCK_LOG"):
            self.addCleanup(setattr, adapter_module, name, getattr(adapter_module, name))
        # Small enough to rotate whichever form the log is in. At 40,000 the block-framed log --
        # roughly a tenth the size of the plain one -- never reached the threshold, and six tests
        # here failed with "nothing rotated": their own guards catching a vacuous run.
        adapter_module.LOCAL_JSONL_MAX_BYTES = 4_000

    def _fill(self, batches: int = 6, per_batch: int = 20) -> list[dict]:
        adapter = MatrixArkLocalAdapter(self.log)
        for batch in range(batches):
            adapter.append_many(_records(batch * per_batch, per_batch))
        _clear_process_read_cache()
        return MatrixArkLocalAdapter(self.log).read_all()

    @staticmethod
    def _is_sealed(path: Path) -> bool:
        """Sealed is the CODEC, not the magic.

        A block-stream shard carries the same magic and is a different thing: appendable, and
        compressed per batch rather than whole-file.
        """
        head = path.read_bytes()[:len(_SHARD_CONTAINER_MAGIC) + 1]
        return (head.startswith(_SHARD_CONTAINER_MAGIC)
                and head[len(_SHARD_CONTAINER_MAGIC):] == _SHARD_CODEC_ZLIB)

    def _rotated(self) -> list[Path]:
        return [path for path in sorted(self.log.parent.iterdir())
                if path.name.startswith("events.jsonl.") and path.suffix.lstrip(".").isdigit()]

    def test_a_plain_rotated_shard_is_sealed(self):
        """Sealing is a whole-file compress of a shard that is finished.

        Pinned with the block log OFF, because that is the case sealing is for: a plain rotated
        shard is 11.66x smaller sealed. A block-stream shard is already compressed and the sealer
        leaves it alone -- see the test below.
        """
        adapter_module.LOCAL_JSONL_BLOCK_LOG = False
        adapter_module.LOCAL_JSONL_COMPRESS_SEALED = True
        self._fill()
        rotated = self._rotated()
        self.assertTrue(rotated, "nothing rotated, so this proves nothing about sealing")
        for path in rotated:
            raw = path.read_bytes()
            self.assertTrue(raw.startswith(_SHARD_CONTAINER_MAGIC),
                            "%s was not sealed" % path.name)
            codec = raw[len(_SHARD_CONTAINER_MAGIC):len(_SHARD_CONTAINER_MAGIC) + 1]
            self.assertEqual(_SHARD_CODEC_ZLIB, codec)
            body = zlib.decompress(raw[len(_SHARD_CONTAINER_MAGIC) + 1:])
            self.assertLess(len(raw), len(body) / 2,
                            "%s is not materially smaller, so it is not earning its format"
                            % path.name)

    def test_a_block_stream_shard_is_left_alone_because_it_is_already_compressed(self):
        """The other half, and why the sealer is not changed to handle it.

        A block-stream shard carries the container magic, so the sealer skips it. Measured, that
        costs 1.09x -- against the 11.66x sealing a PLAIN shard is worth, because the block stream
        is already compressed per batch and lands within 14% of the sealed plain shard. A second
        whole-file pass over compressed bytes for nine percent is a poor trade.
        """
        adapter_module.LOCAL_JSONL_BLOCK_LOG = True
        adapter_module.LOCAL_JSONL_COMPRESS_SEALED = True
        self._fill()
        rotated = self._rotated()
        self.assertTrue(rotated, "nothing rotated, so this proves nothing")
        for path in rotated:
            head = path.read_bytes()[:len(_SHARD_CONTAINER_MAGIC) + 1]
            self.assertTrue(head.startswith(_SHARD_CONTAINER_MAGIC))
            self.assertFalse(self._is_sealed(path),
                             "%s was re-compressed whole; it was already a block stream" % path.name)
            self.assertTrue(list(adapter_module._iter_shard_lines(path)),
                            "%s does not read back" % path.name)

    def test_the_active_shard_is_never_sealed(self):
        """Appends, crash recovery and anyone reading the log by hand all go to this file.

        The invariant is that the SEALER never touches it -- a sealed shard is a whole-file
        compress, and a file still being appended to must never carry one. It is NOT that the
        active shard is plain text: with MATRIXARK_LOCAL_JSONL_BLOCK_LOG on it is a block stream,
        which is appendable and is a different thing from sealed.
        """
        adapter_module.LOCAL_JSONL_COMPRESS_SEALED = True
        self._fill()
        self.assertTrue(self._rotated(), "nothing rotated, so there was no seal to over-reach")
        self.assertFalse(self._is_sealed(self.log),
                         "the ACTIVE shard was sealed; appends land in this file")
        self.assertTrue(list(adapter_module._iter_shard_lines(self.log)),
                        "the active shard is unreadable, whichever form it is in")

    def test_sealing_does_not_change_the_view(self):
        adapter_module.LOCAL_JSONL_COMPRESS_SEALED = False
        plain = self._fill()
        self.assertTrue(self._rotated(), "nothing rotated in the plain arm")

        self._dir.cleanup()
        self._dir = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(self._dir.cleanup)
        self.log = Path(self._dir.name) / "events.jsonl"
        _clear_process_read_cache()
        adapter_module.LOCAL_JSONL_COMPRESS_SEALED = True
        sealed = self._fill()

        self.assertEqual(plain, sealed,
                         "the two storage forms disagree about the record set")

    def test_a_plain_rotated_shard_still_reads(self):
        """No migration: a store rotated before this keeps loading once it is on."""
        adapter_module.LOCAL_JSONL_COMPRESS_SEALED = False
        written = self._fill()
        rotated = self._rotated()
        self.assertTrue(rotated)
        self.assertFalse(self._is_sealed(rotated[0]),
                         "the shard was sealed with sealing turned off")

        adapter_module.LOCAL_JSONL_COMPRESS_SEALED = True
        _clear_process_read_cache()
        self.assertEqual(written, MatrixArkLocalAdapter(self.log).read_all())

    def test_an_unknown_shard_codec_is_refused_rather_than_guessed(self):
        path = Path(self._dir.name) / "sealed.bin"
        path.write_bytes(_SHARD_CONTAINER_MAGIC + b"\x7f" + b"whatever")
        with self.assertRaises(ValueError):
            list(_iter_shard_lines(path))

    def test_the_reader_takes_either_form(self):
        plain = Path(self._dir.name) / "plain.jsonl"
        plain.write_text('{"a":1}\n{"b":2}\n', encoding="utf-8")
        sealed = Path(self._dir.name) / "sealed.jsonl"
        sealed.write_bytes(_SHARD_CONTAINER_MAGIC + _SHARD_CODEC_ZLIB
                           + zlib.compress(b'{"a":1}\n{"b":2}\n', 6))
        self.assertEqual([line.strip() for line in _iter_shard_lines(plain) if line.strip()],
                         [line.strip() for line in _iter_shard_lines(sealed) if line.strip()])

    def test_appending_still_works_after_a_rotation(self):
        adapter_module.LOCAL_JSONL_COMPRESS_SEALED = True
        self._fill()
        self.assertTrue(self._rotated(), "nothing rotated, so nothing was appended past a seal")
        adapter = MatrixArkLocalAdapter(self.log)
        adapter.append_many(_records(9000, 5))
        _clear_process_read_cache()
        view = MatrixArkLocalAdapter(self.log).read_all()
        hashes = {record.get("event_id_hash") for record in view}
        self.assertTrue({9000, 9004}.issubset(hashes),
                        "records appended after a rotation are missing from the view")


    def test_the_active_shard_is_never_handed_to_the_sealer(self):
        """The ORDER is the crash-safety argument, and the end state does not show it.

        Sealing the active log and then renaming it produces exactly the same files as renaming and
        then sealing -- so no assertion about the result can tell them apart. The difference is what
        a crash in between leaves: renaming first leaves a plain rotated shard, which every reader
        accepts; sealing first leaves a COMPRESSED file still named events.jsonl, which the next
        append would extend with plain JSON. This pins the order instead of the result.
        """
        adapter_module.LOCAL_JSONL_COMPRESS_SEALED = True
        seen = []
        original = adapter_module.MatrixArkLocalAdapter._seal_rotated_shard

        def spy(self, path):
            seen.append(Path(path).name)
            return original(self, path)

        adapter_module.MatrixArkLocalAdapter._seal_rotated_shard = spy
        self.addCleanup(setattr, adapter_module.MatrixArkLocalAdapter,
                        "_seal_rotated_shard", original)

        self._fill()
        self.assertTrue(seen, "the sealer never ran, so this proves nothing about what it is given")
        self.assertNotIn("events.jsonl", seen,
                         "the ACTIVE log was handed to the sealer; a crash mid-seal would leave a "
                         "compressed file that the next append extends with plain JSON")

    def test_a_shard_whose_last_line_has_no_newline_is_read_whole(self):
        """Nothing guarantees a shard ends with a newline, and the last record is only reachable
        through the reader's post-loop path."""
        sealed = Path(self._dir.name) / "no-newline.jsonl"
        payload = b'{"a":1}' + b"\n" + b'{"b":2}'
        sealed.write_bytes(_SHARD_CONTAINER_MAGIC + _SHARD_CODEC_ZLIB + zlib.compress(payload, 6))
        self.assertEqual(['{"a":1}', '{"b":2}'],
                         [line.strip() for line in _iter_shard_lines(sealed) if line.strip()])


if __name__ == "__main__":
    unittest.main()
