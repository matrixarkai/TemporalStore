# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A posting keeps the node identities it accumulated, however many times it is folded.

The fold accumulated node and batch identities from the SINGULAR `node_hash` / `batch_id_hash`.
Its own emission writes the singular only when the bucket holds exactly one and pops it otherwise
-- so a posting for a bucket with several nodes carries `node_hashes` and no `node_hash` at all.
Folding that posting again therefore found nothing to carry over and dropped the list:

    after one fold    node_hashes=[200, 201, 202, 203]
    after two folds   node_hashes=None

It was not losing data only while the skip-when-already-folded path returned such a posting
untouched. That path stops firing the moment a fresh row is appended, which is every ingest.

Measured on a live cache of 92 postings, re-folded once: **9 of them came back with no node
identities at all**, and the fix recovers all nine (4 with two identities, 4 with three, 1 with
four). Every other field is unchanged, and the served records are byte-identical.
"""
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import matrixark_mcp_indexing as indexing

BASE = {
    "record_type": "context_index", "index_name": "ix", "capability": "cap",
    "data_model": "dm", "ref_type": "event", "scope_key": "acme|dana",
    "timestamp_key_ms": 1000, "updated_at_ms": 1000,
}


def _rows(count):
    """`count` rows in ONE bucket, each naming a different node."""
    return [dict(BASE, ref_hashes=[100 + i], node_hash=200 + i, node_hashes=[200 + i],
                 batch_id_hash=300 + i, batch_id_hashes=[300 + i])
            for i in range(count)]


def _refold(rows):
    """Fold with the skip path disabled, which is what happens whenever a fresh row is present."""
    saved = indexing._already_folded_postings
    indexing._already_folded_postings = lambda records: None
    try:
        return indexing.compact_context_index_postings([dict(r) for r in rows])
    finally:
        indexing._already_folded_postings = saved


def _posting(rows):
    for record in rows:
        if str(record.get("record_type") or "") == "context_index":
            return record
    return {}


class PostingKeepsItsNodeIdentities(unittest.TestCase):
    def test_a_multi_node_posting_survives_being_folded_again(self):
        for count in (2, 4, 7):
            once = _posting(_refold(_rows(count)))
            self.assertEqual(count, len(once.get("node_hashes") or []),
                             "the first fold did not accumulate %d nodes" % count)
            self.assertIsNone(once.get("node_hash"),
                              "a multi-node posting should carry no singular node_hash")
            twice = _posting(_refold([once]))
            self.assertEqual(once.get("node_hashes"), twice.get("node_hashes"),
                             "folding a %d-node posting again lost its identities" % count)

    def test_batch_identities_survive_too(self):
        """Same accumulation, same spelling problem, same fix."""
        once = _posting(_refold(_rows(4)))
        self.assertEqual(4, len(once.get("batch_id_hashes") or []))
        twice = _posting(_refold([once]))
        self.assertEqual(once.get("batch_id_hashes"), twice.get("batch_id_hashes"))

    def test_a_single_node_posting_is_unchanged(self):
        """The common case must not move: one node still writes the singular and the list."""
        once = _posting(_refold(_rows(1)))
        self.assertEqual([200], once.get("node_hashes"))
        self.assertEqual(200, once.get("node_hash"))
        twice = _posting(_refold([once]))
        self.assertEqual(once.get("node_hashes"), twice.get("node_hashes"))
        self.assertEqual(once.get("node_hash"), twice.get("node_hash"))

    def test_identities_are_not_duplicated_by_reading_both_spellings(self):
        """A row carries the same identity under both names; it must be counted once."""
        row = dict(BASE, ref_hashes=[100], node_hash=200, node_hashes=[200],
                   batch_id_hash=300, batch_id_hashes=[300])
        once = _posting(_refold([row]))
        self.assertEqual([200], once.get("node_hashes"))
        self.assertEqual([300], once.get("batch_id_hashes"))

    def test_the_helper_reads_both_spellings_in_order(self):
        self.assertEqual([1, 2, 3], indexing._identity_values(
            {"node_hashes": [1, 2], "node_hash": 3}, "node_hash", "node_hashes"))
        self.assertEqual([1], indexing._identity_values(
            {"node_hashes": [1], "node_hash": 1}, "node_hash", "node_hashes"))
        self.assertEqual([7], indexing._identity_values(
            {"node_hash": 7}, "node_hash", "node_hashes"))
        self.assertEqual([], indexing._identity_values({}, "node_hash", "node_hashes"))


if __name__ == "__main__":
    unittest.main()
