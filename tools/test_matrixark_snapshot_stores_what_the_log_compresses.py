"""The durable snapshot and the durable log hold the same records; they must hold them the same way.

The log replaces each record's interned metadata with a bundle token and writes one sidecar per
distinct bundle. The snapshot was writing those same records fully expanded, which made the largest
file in the store the one copy of the data that had opted out of the compression -- measured at
3,849 KB against 2,185 KB for 1,225 records.

What these tests pin, in order: the snapshot really is written interned; a reader gets back exactly
what a reader of the log gets, values included; the saving is real rather than a rounding artifact;
and a reader that predates the format is sent to the log instead of being handed tokens it cannot
expand.
"""
import json
import os
import shutil
import tempfile
import unittest
from pathlib import Path

import matrixark_mcp_local_adapter as adapter_module
from matrixark_mcp_local_adapter import (
    _decode_snapshot_bytes,
    INTERN_BUNDLE_TOKEN_KEY,
    INTERN_DICT_RECORD_TYPE,
    LOCAL_DURABLE_READ_CACHE_SCHEMA_VERSION,
    MatrixArkLocalAdapter,
)

SCOPE = {"tenant_id": "acme", "user_id": "dana", "session_id": "skills"}


def skill_text(index: int, sections: int = 12) -> str:
    lines = ["# Runbook %d" % index, "", "A procedure for case %d." % index, ""]
    for step in range(sections):
        lines += ["## Step %d" % step, "",
                  "Check the queue depth for case %d step %d and drain it." % (index, step), ""]
    return "\n".join(lines)


class SnapshotStoresWhatTheLogCompressesTest(unittest.TestCase):

    def setUp(self) -> None:
        self.root = Path(tempfile.mkdtemp(prefix="snapshot_interned_"))
        self.addCleanup(shutil.rmtree, self.root, ignore_errors=True)
        self.log = self.root / "events.jsonl"
        writer = MatrixArkLocalAdapter(self.log)
        for index in range(3):
            writer.ingest({
                "kind": "skill", "scope": SCOPE, "text": skill_text(index),
                "metadata": {"raw_uri": "file:///s/r-%d.md" % index, "title": "r-%d" % index},
            })
        writer.close(timeout_s=600)
        # A read is what materialises the snapshot, so take one and keep what it returned.
        self.from_log = MatrixArkLocalAdapter(self.log).read_all()
        self.base = MatrixArkLocalAdapter(self.log)._durable_read_cache_snapshot_path()
        self.assertTrue(self.base.exists(), "no base snapshot was written, so nothing is under test")

    def _snapshot_records(self) -> list:
        payload = _decode_snapshot_bytes(self.base.read_bytes())
        records = payload.get("records")
        self.assertIsInstance(records, list)
        return records

    def test_the_snapshot_is_written_interned(self) -> None:
        records = self._snapshot_records()
        sidecars = [r for r in records
                    if str(r.get("record_type") or "") == INTERN_DICT_RECORD_TYPE]
        tokened = [r for r in records if INTERN_BUNDLE_TOKEN_KEY in r]
        # Positive control first: a corpus with nothing to intern would satisfy every other
        # assertion here vacuously, so require that interning had something to do.
        self.assertGreater(len(tokened), 0, "no record was interned, so the format is untested")
        self.assertGreater(len(sidecars), 0, "records carry tokens with no sidecar to expand them")
        # And the compression has to be real: far fewer sidecars than the records citing them.
        self.assertLess(len(sidecars), len(tokened),
                        "one sidecar per tokened record stores no less than storing the values")

    def test_a_reader_gets_back_what_the_log_gives(self) -> None:
        from_snapshot = MatrixArkLocalAdapter(self.log).read_all()
        self.assertEqual(len(from_snapshot), len(self.from_log))
        # Compare the records themselves, not their count: expanding to the wrong values, or
        # dropping an interned field, keeps the count exactly right.
        self.assertEqual(
            json.dumps(from_snapshot, sort_keys=True, default=str),
            json.dumps(self.from_log, sort_keys=True, default=str),
        )
        # The interned fields are the ones at risk, so name one and require it present.
        with_route = [r for r in from_snapshot if r.get("storage_route")]
        self.assertGreater(len(with_route), 0,
                           "storage_route survived on no record, so expansion dropped it")
        self.assertNotIn(INTERN_BUNDLE_TOKEN_KEY, from_snapshot[0],
                         "a token reached a caller, which should only ever see expanded records")

    def test_the_snapshot_is_smaller_than_the_expanded_form(self) -> None:
        records = self._snapshot_records()
        interned = len(json.dumps(records, separators=(",", ":"), default=str))
        expanded = len(json.dumps(self.from_log, separators=(",", ":"), default=str))
        self.assertLess(interned, expanded,
                        "the interned snapshot is no smaller than the expanded records")
        # A 1% win would be within noise of the sidecars it adds; require the saving to be worth
        # the format change. Measured at 40-43% across corpus sizes.
        self.assertLess(interned, expanded * 0.85,
                        "saving is %.1f%%, too small to justify the stored format"
                        % (100.0 * (expanded - interned) / expanded))

    def test_an_older_reader_is_sent_back_to_the_log(self) -> None:
        head_path = self.root / ".events.jsonl.read-cache-head.json"
        head = json.loads(head_path.read_text(encoding="utf-8"))
        self.assertEqual(head.get("schema_version"), LOCAL_DURABLE_READ_CACHE_SCHEMA_VERSION)
        # Stamp the head with the version that predates interning, which is what an older writer
        # would have left, and confirm the loader declines it rather than expanding nothing.
        head["schema_version"] = LOCAL_DURABLE_READ_CACHE_SCHEMA_VERSION - 1
        head_path.write_text(json.dumps(head, separators=(",", ":")) + "\n", encoding="utf-8")
        reader = MatrixArkLocalAdapter(self.log)
        loaded = reader._load_durable_read_cache({"total_size": os.path.getsize(self.log)})
        self.assertIsNone(loaded, "a snapshot from an unknown format version was accepted")
        # The store still serves, because declining the snapshot falls back to the log.
        self.assertEqual(
            json.dumps(reader.read_all(), sort_keys=True, default=str),
            json.dumps(self.from_log, sort_keys=True, default=str),
        )

    def test_the_expansion_shares_one_object_per_value(self) -> None:
        records = MatrixArkLocalAdapter(self.log).read_all()
        holders = [r for r in records if isinstance(r.get("storage_route"), (dict, list, str))]
        self.assertGreater(len(holders), 4, "too few records carry the field to show sharing")
        objects = {id(r["storage_route"]) for r in holders}
        self.assertLess(len(objects), len(holders),
                        "%d records hold %d separate copies of storage_route"
                        % (len(holders), len(objects)))


if __name__ == "__main__":
    unittest.main()
