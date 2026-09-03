"""The durable snapshot must decode to the same objects the log decodes to.

Both paths return the same records, so they have to return them at the same cost. A bare
json.load on the snapshot gives every record a private copy of every repeated VALUE -- so a
store served from its snapshot held a cache a third larger than the same store served from its
log, for byte-identical content.
"""
import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import matrixark_mcp_local_adapter as adapter_module


SHARED_TEXT = "the same section body, stored under two record types"


class SnapshotDecodesLikeTheLog(unittest.TestCase):
    def _store_with_a_snapshot(self):
        store = Path(tempfile.mkdtemp())
        log = store / "events.jsonl"
        rows = [
            {"record_type": "skill_section", "node_id": "n-%d" % i, "text": SHARED_TEXT}
            for i in range(6)
        ]
        head = {"schema_version": adapter_module.LOCAL_DURABLE_READ_CACHE_SCHEMA_VERSION,
                "record_count": len(rows), "delta_count": 0}
        log.write_text("".join(json.dumps(r) + "\n" for r in rows), encoding="utf-8")
        snap = log.with_name(log.name + ".read-cache.json")
        snap.write_text(json.dumps({"records": rows}), encoding="utf-8")
        return store, log, snap, head, rows

    def _decode_snapshot(self, snap):
        """Decode through the adapter's own snapshot reader, not a hand-rolled copy."""
        store, log, real_snap, head, rows = self._store_with_a_snapshot()
        adapter = adapter_module.MatrixArkLocalAdapter(log)
        head["cache_key"] = adapter._cache_key_str()
        signature = adapter._jsonl_cache_signature_detail()
        head["signature"] = signature
        adapter._durable_read_cache_head_path().write_text(json.dumps(head), encoding="utf-8")
        loaded = adapter._load_durable_read_cache(signature)
        return loaded, rows

    def test_the_snapshot_reader_returns_records(self):
        """Non-vacuity: if the reader rejects the snapshot every other assertion passes emptily."""
        loaded, rows = self._decode_snapshot(None)
        self.assertIsNotNone(loaded, "the snapshot reader rejected the snapshot, so the sharing "
                                     "assertions below would pass against nothing")
        self.assertEqual(len(loaded), len(rows))
        self.assertTrue(all(r["text"] == SHARED_TEXT for r in loaded))

    def test_a_repeated_value_is_one_object(self):
        loaded, _ = self._decode_snapshot(None)
        self.assertIsNotNone(loaded)
        objects = {id(record["text"]) for record in loaded}
        self.assertEqual(
            len(objects), 1,
            "%d records carry the same text and the snapshot decoded it into %d separate "
            "objects; the log path decodes it into one" % (len(loaded), len(objects)),
        )

    def test_key_names_are_interned(self):
        """This one held before the value fix too, and it is worth saying why.

        The snapshot is decoded in a SINGLE json.load call, and the decoder memoises key
        strings within a call -- so keys were already shared here and only values were not. That
        is what made the duplication easy to miss. Pinned so a future change that decodes the
        snapshot incrementally does not quietly lose the property.
        """
        loaded, _ = self._decode_snapshot(None)
        self.assertIsNotNone(loaded)
        for name in ("record_type", "node_id", "text"):
            objects = {id(key) for record in loaded for key in record if key == name}
            self.assertEqual(
                len(objects), 1,
                "key %r was decoded into %d separate string objects" % (name, len(objects)),
            )


if __name__ == "__main__":
    unittest.main()
