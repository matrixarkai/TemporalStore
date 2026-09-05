# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""The active log can be written as a stream of blocks, one block per append batch.

At 76 KB documents the event log is 95.3% of everything this module writes -- the snapshot, its
tail and the rotated shards are all compressed now, and the log is the last plain-text artifact.
Priced against the batches this code really appends (3, 5 and 284 records, recorded by spying on
append_many):

    plain JSONL (today)                     4,853,970    1.00x
    one block per RECORD                    3,399,456    1.43x
    one block per APPEND BATCH                501,381    9.68x
    one block per 256 records                 485,806    9.99x
    the whole file at once (a ceiling)        276,828   17.53x

A block per append batch reaches 97% of what fixed 256-record blocks would, and costs NO durability:
the batch is already the unit acked together, so a crash loses exactly what it loses today.

OFF by default, and this one is not a formality. Every other change in this family compresses
something DERIVED -- a snapshot, a tail, a sealed shard -- which can always be rebuilt from the log.
This changes the log itself, and a process appending plain JSON to a block-framed log corrupts it.
So the form is taken from the file on disk rather than from the flag: an existing log keeps its own
form, only a fresh one adopts the configured one, and turning the flag off leaves every log readable.
"""
import json
import tempfile
import unittest
from pathlib import Path

from tools import matrixark_mcp_local_adapter as adapter_module
from tools.matrixark_mcp_local_adapter import (
    _SHARD_CODEC_BLOCKS,
    _SHARD_CONTAINER_MAGIC,
    _encode_log_block,
    _iter_shard_lines,
    loads_with_interned_keys,
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
            "text": "event %d " % index + "x" * 300,
            "nested": {"a": [1, 2, {"b": "c"}], "unicode": "中文 — dash"},
            "updated_at_ms": 1780000000000 + index,
        }
        for index in range(start, start + count)
    ]


class LogIsWrittenInBlocksTest(unittest.TestCase):
    def setUp(self) -> None:
        self._dir = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(self._dir.cleanup)
        self.store = Path(self._dir.name)
        self.log = self.store / "events.jsonl"
        _clear_process_read_cache()
        self.addCleanup(_clear_process_read_cache)
        self.addCleanup(setattr, adapter_module, "LOCAL_JSONL_BLOCK_LOG",
                        adapter_module.LOCAL_JSONL_BLOCK_LOG)

    def _read_back(self, path: Path) -> list[dict]:
        return [loads_with_interned_keys(line) for line in _iter_shard_lines(path)
                if line.strip()]

    def _appended(self, path: Path) -> list[dict]:
        """Only the records this test appended.

        `_encode_records_for_log` puts intern-dictionary sidecars in the log beside them -- a
        storage detail, not part of the history -- so a raw line count is one or two more than what
        was written, and an assertion on it would be measuring the interner.
        """
        return [record for record in self._read_back(path)
                if record.get("record_type") == "context_event"]

    def test_the_log_says_what_it_is_and_is_much_smaller(self):
        adapter_module.LOCAL_JSONL_BLOCK_LOG = True
        adapter = MatrixArkLocalAdapter(self.log)
        adapter.append_many(_records(0, 200))
        raw = self.log.read_bytes()
        self.assertTrue(raw.startswith(_SHARD_CONTAINER_MAGIC + _SHARD_CODEC_BLOCKS),
                        "the log does not declare the block-stream form")

        plain = self.store / "plain.jsonl"
        with plain.open("w", encoding="utf-8") as handle:
            for record in self._read_back(self.log):
                handle.write(json.dumps(record, separators=(",", ":")) + "\n")
        self.assertLess(len(raw), plain.stat().st_size / 4,
                        "the block log is not materially smaller, so it is not earning its format")

    def test_the_log_holds_the_same_records_either_way(self):
        """Checked here rather than end to end: context_index postings compact differently run to
        run, so a served-record count moves by a dozen for reasons that have nothing to do with the
        format -- it moved, and then moved back, while this was being written."""
        batches = [_records(0, 3), _records(3, 5), _records(8, 284)]
        records = [record for batch in batches for record in batch]

        plain = self.store / "plain.jsonl"
        with plain.open("w", encoding="utf-8") as handle:
            for record in records:
                handle.write(json.dumps(record, separators=(",", ":")) + "\n")

        blocks = self.store / "blocks.jsonl"
        with blocks.open("wb") as handle:
            handle.write(_SHARD_CONTAINER_MAGIC + _SHARD_CODEC_BLOCKS)
            for batch in batches:
                handle.write(_encode_log_block(batch))

        self.assertEqual(self._read_back(plain), self._read_back(blocks))
        self.assertEqual(records, self._read_back(blocks))

    def test_one_block_per_append_batch(self):
        """The durability argument in one assertion.

        A block is only free of durability cost while it is exactly what an append already acked
        together. A writer that buffered across batches would compress better and would quietly
        widen the window a crash can lose.
        """
        adapter_module.LOCAL_JSONL_BLOCK_LOG = True
        adapter = MatrixArkLocalAdapter(self.log)
        for batch in (_records(0, 3), _records(3, 5), _records(8, 40)):
            adapter.append_many(batch)

        raw = self.log.read_bytes()[len(_SHARD_CONTAINER_MAGIC) + 1:]
        blocks, at = 0, 0
        while at + 5 <= len(raw):
            length = int.from_bytes(raw[at + 1:at + 5], "big")
            at += 5 + length
            blocks += 1
        self.assertEqual(3, blocks, "the log does not hold one block per append batch")

    def test_a_plain_log_already_on_disk_stays_plain(self):
        """The form comes from the FILE, which is what keeps two forms out of one of them."""
        adapter_module.LOCAL_JSONL_BLOCK_LOG = False
        MatrixArkLocalAdapter(self.log).append_many(_records(0, 4))
        self.assertTrue(self.log.read_bytes().startswith(b"{"))

        adapter_module.LOCAL_JSONL_BLOCK_LOG = True
        _clear_process_read_cache()
        MatrixArkLocalAdapter(self.log).append_many(_records(4, 4))
        self.assertTrue(self.log.read_bytes().startswith(b"{"),
                        "turning the flag on converted a plain log mid-life; a reader that only "
                        "knows the plain form would stop at the first block")
        self.assertEqual(8, len(self._appended(self.log)))

    def test_a_block_log_stays_blocks_when_the_flag_goes_off(self):
        adapter_module.LOCAL_JSONL_BLOCK_LOG = True
        MatrixArkLocalAdapter(self.log).append_many(_records(0, 4))
        self.assertTrue(self.log.read_bytes().startswith(_SHARD_CONTAINER_MAGIC))

        adapter_module.LOCAL_JSONL_BLOCK_LOG = False
        _clear_process_read_cache()
        MatrixArkLocalAdapter(self.log).append_many(_records(4, 4))
        self.assertTrue(self.log.read_bytes().startswith(_SHARD_CONTAINER_MAGIC),
                        "plain lines were appended onto a block log, which corrupts it")
        self.assertEqual(8, len(self._appended(self.log)))

    def test_a_torn_final_block_drops_only_its_own_records(self):
        """The same contract a half-written line has: a crash mid-append loses that append."""
        adapter_module.LOCAL_JSONL_BLOCK_LOG = True
        adapter = MatrixArkLocalAdapter(self.log)
        adapter.append_many(_records(0, 5))
        adapter.append_many(_records(5, 5))
        whole = self._appended(self.log)
        self.assertEqual(10, len(whole))

        raw = self.log.read_bytes()
        self.log.write_bytes(raw[: len(raw) - 20])
        kept = self._appended(self.log)
        self.assertEqual(whole[:len(kept)], kept, "the survivors are not a prefix of the log")
        self.assertLess(len(kept), len(whole))
        self.assertGreaterEqual(len(kept), 5, "an intact earlier block was lost with the torn one")

    def test_a_cold_read_serves_the_block_log(self):
        adapter_module.LOCAL_JSONL_BLOCK_LOG = True
        adapter = MatrixArkLocalAdapter(self.log)
        adapter.append_many(_records(0, 40))
        expected = adapter.read_all()

        for path in list(self.store.iterdir()):
            if "read-cache" in path.name:
                path.unlink()
        _clear_process_read_cache()
        self.assertEqual(expected, MatrixArkLocalAdapter(self.log).read_all())


    def test_a_block_log_rotates_into_a_sealed_shard_and_starts_a_fresh_one(self):
        """Three formats meet here and none of them may swallow another.

        The active log is a block stream; rotation renames it to a shard, which the sealer then
        compresses WHOLE; and the new active log must start again with its own magic rather than
        inherit the old file's bytes. A reader has to walk all three and return every record.
        """
        adapter_module.LOCAL_JSONL_BLOCK_LOG = True
        adapter_module.LOCAL_JSONL_COMPRESS_SEALED = True
        self.addCleanup(setattr, adapter_module, "LOCAL_JSONL_MAX_BYTES",
                        adapter_module.LOCAL_JSONL_MAX_BYTES)
        adapter_module.LOCAL_JSONL_MAX_BYTES = 4096

        adapter = MatrixArkLocalAdapter(self.log)
        for start in range(0, 400, 40):
            adapter.append_many(_records(start, 40))

        rotated = [path for path in sorted(self.store.iterdir())
                   if path.name.startswith("events.jsonl.") and path.suffix.lstrip(".").isdigit()]
        self.assertTrue(rotated, "nothing rotated, so this tests none of the seam")
        self.assertTrue(self.log.read_bytes().startswith(_SHARD_CONTAINER_MAGIC + _SHARD_CODEC_BLOCKS),
                        "the log after rotation is not a fresh block stream")

        # In the module's own order: `.1` is the MOST RECENT rotated shard, because rotation
        # shifts `.1` to `.2`, so reading them by filename returns history backwards.
        seen = []
        for path in MatrixArkLocalAdapter(self.log)._retained_jsonl_paths():
            seen.extend(record["event_id_hash"] for record in self._appended(path))
        retained = adapter_module.LOCAL_JSONL_RETENTION_COUNT
        self.assertEqual(sorted(seen), seen, "records came back out of order across the shards")
        self.assertEqual(len(set(seen)), len(seen), "a record was returned twice across the seam")
        self.assertIn(399, seen, "the most recent record is missing from the retained shards")
        self.assertGreaterEqual(len(rotated) + 1, 2)
        self.assertLessEqual(len(rotated) + 1, retained)


if __name__ == "__main__":
    unittest.main()
