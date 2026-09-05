# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""The snapshot no longer needs the persisted prefix to still be a prefix.

The tail could only ever be appended when the live list was an EXTENSION of what was on disk.
Ingest compacts, and compaction removes records from the middle, so it usually was not -- and the
fallback rewrote the entire record set as JSON on the read that followed every ingest.

The delta is written by the APPEND path instead, carrying the records it was actually handed, and
the load path compacts what it stitches. A base holding a row a later append superseded is then
reconciled rather than wrong, so the writer can stop asking the question that forced the rewrite.

What each test here guards, and how to check it still guards it:

  * compacting at load          -- drop it and test_a_delete_survives_the_base_delta_seam fails
  * the pre_size guard          -- drop it and the interloper test in test_incremental_read_cache
                                   fails, serving 55 records where the log holds 75
  * writing the delta on append -- drop it and test_a_read_leaves_a_current_snapshot_alone fails
"""
import json
import tempfile
import unittest
from pathlib import Path

from tools.matrixark_mcp_local_adapter import (
    MatrixArkLocalAdapter,
    _LOCAL_READ_CACHE,
    _LOCAL_READ_CACHE_LOCK,
)

SCOPE = {"tenant_id": "acme", "user_id": "dana", "session_id": "skills"}


def _clear_process_read_cache() -> None:
    with _LOCAL_READ_CACHE_LOCK:
        _LOCAL_READ_CACHE.clear()


def _body(tag: str) -> str:
    return "\n".join(["# %s" % tag, ""] + ["Step %d for %s." % (i, tag) for i in range(12)])


class SnapshotDoesNotNeedAPrefixTest(unittest.TestCase):
    def setUp(self) -> None:
        self._dir = tempfile.TemporaryDirectory()
        self.addCleanup(self._dir.cleanup)
        self.store = Path(self._dir.name)
        self.log = self.store / "events.jsonl"
        self.base = self.store / "events.jsonl.read-cache.json"
        self.delta = self.store / ".events.jsonl.read-cache-delta.jsonl"
        _clear_process_read_cache()
        self.addCleanup(_clear_process_read_cache)

    # -- helpers ---------------------------------------------------------------------------
    def _ingest(self, tag, uri=None):
        adapter = MatrixArkLocalAdapter(self.log)
        result = adapter.ingest({
            "kind": "skill",
            "scope": SCOPE,
            "text": _body(tag),
            "metadata": {"raw_uri": uri or ("file:///s/%s.md" % tag), "title": tag},
        })
        adapter.close(timeout_s=3600)
        return result

    def _stamp(self):
        if not self.base.exists():
            return None
        stat = self.base.stat()
        return (stat.st_size, stat.st_mtime_ns)

    def _delta_lines(self):
        if not self.delta.exists():
            return 0
        with self.delta.open("r", encoding="utf-8") as handle:
            return sum(1 for line in handle if line.strip())

    def _snapshot_view(self):
        """The view a cold reader is served, with the source it came from."""
        _clear_process_read_cache()
        reader = MatrixArkLocalAdapter(self.log)
        return reader.read_all(), getattr(reader, "_read_cache_source", "?")

    def _log_view(self):
        """The same store rebuilt from the log alone, then restored exactly as it was."""
        names = (
            "events.jsonl.read-cache.json",
            ".events.jsonl.read-cache-delta.jsonl",
            ".events.jsonl.read-cache-head.json",
        )
        saved = {n: (self.store / n).read_bytes() for n in names if (self.store / n).exists()}
        for name in saved:
            (self.store / name).unlink()
        _clear_process_read_cache()
        try:
            return MatrixArkLocalAdapter(self.log).read_all()
        finally:
            for name in names:
                if (self.store / name).exists():
                    (self.store / name).unlink()
            for name, blob in saved.items():
                (self.store / name).write_bytes(blob)
            _clear_process_read_cache()

    def _mentions(self, view, tag):
        return sum(1 for record in view if tag in json.dumps(record, default=str))

    def _delete(self, result):
        memory_id = str((result or {}).get("event_id_hash") or (result or {}).get("id") or "")
        self.assertTrue(memory_id, "ingest returned no id to delete")
        adapter = MatrixArkLocalAdapter(self.log)
        matched = len(adapter.records_for_delete(memory_id))
        self.assertGreater(matched, 0, "the delete matched nothing, so it tombstones nothing")
        adapter.delete_memory({"memory_id": memory_id})
        adapter.close(timeout_s=3600)

    # -- tests -----------------------------------------------------------------------------
    def test_a_read_leaves_a_current_snapshot_alone(self):
        """The read that follows an ingest used to rewrite the whole base.

        The append writes its own records to the delta and stamps the head with the signature
        they produced, so the read that follows finds the snapshot already describing this log
        and has nothing to do. Asserted on the FILE, because that is where the cost was.
        """
        self._ingest("first")
        MatrixArkLocalAdapter(self.log).read_all()          # checkpoint a base to extend
        self.assertTrue(self.base.exists(), "no base was written, so this test cannot bind")
        before, delta_before = self._stamp(), self._delta_lines()

        self._ingest("second")
        self.assertGreater(self._delta_lines(), delta_before,
                           "the append wrote nothing to the delta")
        self.assertEqual(before, self._stamp(),
                         "the append rewrote the base instead of extending the delta")

        _clear_process_read_cache()
        MatrixArkLocalAdapter(self.log).read_all()
        self.assertEqual(before, self._stamp(),
                         "the read rewrote a base that already described this log")

    def test_a_cold_reader_is_served_the_log_view(self):
        """Base plus delta, compacted, is the view the log gives -- and it really came from the
        snapshot, or every other assertion here passes emptily."""
        self._ingest("alpha")
        MatrixArkLocalAdapter(self.log).read_all()
        for tag in ("beta", "gamma", "epsilon"):
            self._ingest(tag)

        view, source = self._snapshot_view()
        self.assertEqual(source, "durable", "the snapshot did not serve the read")
        self.assertGreater(self._delta_lines(), 0, "nothing was stitched, so the seam is untested")
        self.assertEqual(self._log_view(), view)
        for tag in ("alpha", "beta", "gamma", "epsilon"):
            self.assertTrue(self._mentions(view, tag), "%s is missing from the cold view" % tag)

    def test_a_delete_survives_the_base_delta_seam(self):
        """A tombstone in the DELTA must remove a record held in the BASE.

        Tombstones only remove records appearing before them, and compaction strips the markers,
        so a delete has to stay visible across the seam. Without compacting at load, the deleted
        document comes back after a restart.
        """
        result = self._ingest("doomed")
        MatrixArkLocalAdapter(self.log).read_all()          # doomed is now IN the base
        view, _ = self._snapshot_view()
        self.assertTrue(self._mentions(view, "doomed"),
                        "the document never reached the base, so the delete proves nothing")

        self._delete(result)

        view, _ = self._snapshot_view()
        from_log = self._log_view()
        self.assertEqual(from_log, view)
        self.assertEqual(self._mentions(view, "doomed"), self._mentions(from_log, "doomed"),
                         "the snapshot and the log disagree about a deleted document")

    def test_a_record_written_after_a_tombstone_survives(self):
        """The reverse ordering: a tombstone folded into the base must not remove a record
        appended after it, even when that record reuses the deleted document's uri."""
        result = self._ingest("condemned", uri="file:///s/reused.md")
        MatrixArkLocalAdapter(self.log).read_all()
        self._delete(result)
        MatrixArkLocalAdapter(self.log).read_all()          # fold the tombstone into the base

        self._ingest("survivor", uri="file:///s/reused.md")
        view, _ = self._snapshot_view()
        self.assertEqual(self._log_view(), view)
        self.assertTrue(self._mentions(view, "survivor"),
                        "a tombstone in the base removed a record appended after it")

    def test_the_delta_stays_within_its_cap(self):
        """The delta is bounded: past the cap the base is folded forward again, so a long-lived
        store does not accumulate an unbounded tail for every load to stitch."""
        from tools.matrixark_mcp_local_adapter import LOCAL_DURABLE_READ_CACHE_MAX_DELTA

        self._ingest("seed")
        MatrixArkLocalAdapter(self.log).read_all()
        peak = 0
        for index in range(12):
            self._ingest("doc-%d" % index)
            MatrixArkLocalAdapter(self.log).read_all()
            peak = max(peak, self._delta_lines())
        self.assertGreater(peak, 0, "the delta was never used, so the bound is untested")
        self.assertLessEqual(peak, LOCAL_DURABLE_READ_CACHE_MAX_DELTA)

        view, source = self._snapshot_view()
        self.assertEqual(source, "durable")
        self.assertEqual(self._log_view(), view)


if __name__ == "__main__":
    unittest.main()
