# SPDX-License-Identifier: Apache-2.0
"""The durable read cache writes appends as a tail, not a full rewrite.

Every failure these pin was hit for real while building the tail path:
  * a slice-based tail drifts once compaction removes an earlier record;
  * a sidecar under the event log name prefix is replayed as durable history;
  * a second adapter over the same log leaves the tail at the wrong offset.
The cache is only worth having if the view it serves still equals the log.
"""
from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import matrixark_mcp_local_adapter as A


def _records(start: int, count: int) -> list[dict]:
    return [
        {
            "record_type": "context_event",
            "event_id_hash": index,
            "text": f"event {index} " + ("x" * 200),
            "updated_at_ms": 1780000000000 + index,
        }
        for index in range(start, start + count)
    ]


class IncrementalReadCacheCase(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.dir = Path(self._tmp.name)
        self.path = self.dir / "events.jsonl"
        self.SEED = 50
        A._LOCAL_READ_CACHE.clear()
        self.addCleanup(A._LOCAL_READ_CACHE.clear)
        self.addCleanup(self._tmp.cleanup)

    def _primed(self) -> A.MatrixArkLocalAdapter:
        """Adapter with the durable base established. The cache is written from the cached record
        list, which only exists after a read -- without this the tail path is never exercised and
        the assertions below quietly skip."""
        adapter = A.MatrixArkLocalAdapter(self.path)
        adapter.append_many(_records(0, self.SEED))
        adapter.read_all()
        self.assertTrue(adapter._durable_read_cache_snapshot_path().exists(),
                        "priming read did not establish a durable base")
        return adapter

    def _fresh_view(self) -> list[dict]:
        """What a cold reader sees. The process-global cache is cleared so the answer has to
        come from the durable files (or the log), never from memory in this process."""
        A._LOCAL_READ_CACHE.clear()
        return A.MatrixArkLocalAdapter(self.path).read_all()

    def _log_view(self) -> list[dict]:
        adapter = A.MatrixArkLocalAdapter(self.path)
        adapter._clear_jsonl_read_caches()
        A._LOCAL_READ_CACHE.clear()
        return A.MatrixArkLocalAdapter(self.path).read_all()

    def test_appends_do_not_rewrite_the_base(self):
        """The point of the whole change: appending must not re-serialize the corpus."""
        adapter = self._primed()
        base = adapter._durable_read_cache_snapshot_path()
        before_bytes = base.read_bytes()

        for record in _records(50, 10):
            adapter.append(record)

        self.assertEqual(before_bytes, base.read_bytes(),
                         "base was rewritten -- the append path fell back to a full write")
        self.assertTrue(adapter._durable_read_cache_tail_path().exists(),
                        "nothing was written to the tail")
        self.assertEqual(60, len(self._fresh_view()),
                         "a cold reader must see the appended records")

    def test_cold_view_matches_the_log(self):
        adapter = self._primed()
        for record in _records(self.SEED, 25):
            adapter.append(record)
        self.assertEqual(self._log_view(), self._fresh_view())

    def test_sidecars_are_not_durable_shards(self):
        """Callers glob the log name prefix to enumerate retained shards; a cache file caught by
        that glob is replayed as history. This is how the tail file first corrupted reads."""
        adapter = self._primed()
        for record in _records(self.SEED, 5):
            adapter.append(record)
        globbed = sorted(p.name for p in self.dir.glob(self.path.name + "*"))
        for name in globbed:
            self.assertNotIn("read-cache-delta", name)
            self.assertNotIn("read-cache-head", name)
        self.assertNotIn(adapter._durable_read_cache_tail_path().name, globbed)
        self.assertNotIn(adapter._durable_read_cache_head_path().name, globbed)

    def test_a_second_writer_does_not_corrupt_the_view(self):
        """Two adapters over one log each hold their own record list, and the signature alone
        cannot catch a stale one -- it describes the log at WRITE time, so whichever adapter
        wrote last used to stamp the current signature onto a view missing the other's records,
        and a cold reader silently lost them (55 of 75 here). The append path now compares the
        bytes its cached view covers against the log as it stood before its own write, and
        refuses to publish a view that was already behind.
        """
        first = self._primed()
        second = A.MatrixArkLocalAdapter(self.path)
        second.append_many(_records(self.SEED, 20))
        second.read_all()
        for record in _records(self.SEED + 20, 5):
            first.append(record)
        total = self.SEED + 25
        view = self._fresh_view()
        self.assertEqual(total, len(view))
        self.assertEqual(list(range(total)), sorted(r["event_id_hash"] for r in view))
        self.assertEqual(self._log_view(), view)

    def test_a_writer_in_another_process_does_not_corrupt_the_view(self):
        """Same staleness, harder case: the interloper's records never pass through this
        process, so the shared in-process cache cannot repair the view -- the only tell is
        that the log grew by bytes this instance never accounted for. Simulated by writing
        lines straight to the log file between two adapters' appends.
        """
        adapter = self._primed()
        with self.path.open("a", encoding="utf-8") as handle:
            for record in _records(self.SEED, 20):
                handle.write(json.dumps(record, separators=(",", ":")) + "\n")
        for record in _records(self.SEED + 20, 5):
            adapter.append(record)
        total = self.SEED + 25
        view = self._fresh_view()
        self.assertEqual(total, len(view))
        self.assertEqual(list(range(total)), sorted(r["event_id_hash"] for r in view))
        self.assertEqual(self._log_view(), view)

    def test_deletion_does_not_leave_a_stale_tail(self):
        """Compaction rewrites the list, so the persisted prefix no longer holds -- the writer
        must fall back to a full rewrite rather than append against a moved offset."""
        adapter = self._primed()
        for record in _records(self.SEED, 5):
            adapter.append(record)
        adapter.append({
            "record_type": "context_event",
            "event_id_hash": 7,
            "text": "replacement for 7",
            "updated_at_ms": 1790000000000,
        })
        view = self._fresh_view()
        self.assertEqual(self._log_view(), view)
        seven = [r for r in view if r.get("event_id_hash") == 7]
        self.assertEqual(1, len(seven), "the superseded record must not survive in the cache")
        self.assertEqual("replacement for 7", seven[0]["text"])

    def test_clearing_removes_every_cache_file(self):
        """A stale head or tail outliving the base is worse than no cache at all."""
        adapter = self._primed()
        for record in _records(self.SEED, 5):
            adapter.append(record)
        adapter._clear_jsonl_read_caches()
        for cache_file in (
            adapter._durable_read_cache_snapshot_path(),
            adapter._durable_read_cache_tail_path(),
            adapter._durable_read_cache_head_path(),
        ):
            self.assertFalse(cache_file.exists(), f"{cache_file.name} survived the clear")

    def test_a_truncated_tail_is_refused_not_served(self):
        """A half-written tail line must invalidate the cache, never shorten the view."""
        adapter = self._primed()
        for record in _records(self.SEED, 8):
            adapter.append(record)
        delta = adapter._durable_read_cache_tail_path()
        self.assertTrue(delta.exists(), "no tail was written -- nothing to damage")
        # Damage the LAST bytes, whatever the tail is encoded as. Reading it as text would fail
        # outright on a block-framed tail, which would test the harness rather than the loader.
        raw = delta.read_bytes()
        delta.write_bytes(raw[: max(1, len(raw) - 20)])
        A._LOCAL_READ_CACHE.clear()
        view = A.MatrixArkLocalAdapter(self.path).read_all()
        self.assertEqual(self.SEED + 8, len(view),
                         "a damaged tail must fall back to the log, not truncate")

    def test_head_counts_track_what_was_persisted(self):
        adapter = self._primed()
        for record in _records(self.SEED, 6):
            adapter.append(record)
        head_path = adapter._durable_read_cache_head_path()
        self.assertTrue(head_path.exists(), "no head was written")
        head = json.loads(head_path.read_text(encoding="utf-8"))
        # Through the module's decoder, so this reads the snapshot however it is stored.
        base = A._decode_snapshot_bytes(
            adapter._durable_read_cache_snapshot_path().read_bytes())
        # `delta_count` counts RECORDS. While the tail was one document per line those were the
        # same number; a block-framed tail holds many records per block and its payload contains
        # newlines of its own, so read it the way the loader does.
        tail_path = adapter._durable_read_cache_tail_path()
        tail_bytes = tail_path.read_bytes()
        if tail_path.suffix == ".bin":
            tail = A._decode_delta_blocks(tail_bytes)
        else:
            tail = [l for l in tail_bytes.decode("utf-8").splitlines() if l.strip()]
        # `record_count` counts DATA records, which is what the load path recovers and compares
        # after expansion. The stored array is longer by the intern sidecars it carries, so count
        # what the head is actually describing rather than the raw array length.
        stored = [r for r in base["records"]
                  if str(r.get("record_type") or "") != A.INTERN_DICT_RECORD_TYPE]
        self.assertEqual(head["record_count"], len(stored))
        # Stronger than the length check it replaces: expanding the array has to yield exactly the
        # count the head claims, which a stray sidecar or a dropped record would break.
        self.assertEqual(head["record_count"],
                         len(A.expand_interned_records(list(base["records"]))))
        self.assertEqual(head["delta_count"], len(tail))
        self.assertEqual(self.SEED + 6, head["record_count"] + head["delta_count"])


if __name__ == "__main__":
    unittest.main()
