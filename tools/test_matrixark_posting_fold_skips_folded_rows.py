# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""The posting fold does not re-fold rows it already folded.

Compaction re-folds the read cache on every read, and the cache holds the fold's OWN output. The
fold coalesces nothing at any corpus size -- 63 rows to 63 postings, 113 to 113, 213 to 213, 413
to 413 -- yet it rebuilt each one, at about 57 microseconds of grouping plus 9 of identity hashing
per posting.

The fold is idempotent, so when every posting is already its own output and no two share a bucket,
the answer is the input. Measured paired against the live chain on 1,423 records:

  the state stage    18.356 -> 5.128 ms   -72%
  whole compaction   23.993 -> 10.493 ms  -56%
  skill ingest       147.5 -> 88.7 ms/skill at 200 skills, -40%

What makes this safe is the equivalence check below rather than the argument above: the fast path
must return exactly what the full pass returns. It caught a first version that accepted any row
carrying the policy string, when the fold also stamps index_hash and storage fields and drops
empty hash lists.
"""
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import matrixark_mcp_indexing as indexing
import matrixark_mcp_local_adapter as adapter_module

_BLANK = chr(10) + chr(10)


def _norm(rows):
    return [json.dumps(r, sort_keys=True, default=str) for r in rows]


def _full_fold(rows):
    """The pass with the fast path disabled -- the answer everything is checked against."""
    saved = indexing._already_folded_postings
    indexing._already_folded_postings = lambda records: None
    try:
        return indexing.compact_context_index_postings(list(rows))
    finally:
        indexing._already_folded_postings = saved


def _posting(bucket, refs, folded=True):
    row = {
        "record_type": "context_index", "index_name": "ix%d" % bucket, "capability": "cap",
        "data_model": "dm", "ref_type": "event", "scope_key": "acme|dana",
        "timestamp_key_ms": 1000 * bucket, "updated_at_ms": 1000 * bucket,
        "ref_hashes": list(refs), "posting_count": len(refs), "posting_part": 0,
    }
    if folded:
        row.update({"posting_policy": indexing.POSTING_POLICY_BUCKETED,
                    "index_hash": 12345 + bucket,
                    "storage_record_kind": "index", "storage_part": "index"})
    return row


def _skill_text(index, sections=5):
    parts = ["# Runbook %d" % index, "A procedure for case %d." % index]
    for step in range(sections):
        parts.append("## Step %d" % step)
        parts.append("Drain the queue for case %d step %d." % (index, step))
    return _BLANK.join(parts)


def _real_records(count=12):
    with adapter_module._LOCAL_READ_CACHE_LOCK:
        adapter_module._LOCAL_READ_CACHE.clear()
    adapter = adapter_module.MatrixArkLocalAdapter(Path(tempfile.mkdtemp()) / "events.jsonl")
    scope = {"tenant_id": "acme", "user_id": "dana", "session_id": "skills"}
    for i in range(count):
        adapter.ingest({"kind": "skill", "scope": scope, "text": _skill_text(i),
                        "metadata": {"raw_uri": "file:///s/r-%05d.md" % i, "title": "r-%05d" % i}})
    return adapter.read_all()


class PostingFoldSkipsFoldedRows(unittest.TestCase):
    def test_it_returns_exactly_what_the_full_pass_returns(self):
        records = _real_records()
        self.assertIsNotNone(indexing._already_folded_postings(list(records)),
                             "the fast path did not fire, so this compares nothing")
        self.assertEqual(_norm(_full_fold(records)),
                         _norm(indexing.compact_context_index_postings(list(records))))

    def test_the_cache_really_does_hold_the_folds_own_output(self):
        """The premise. If the cache stopped holding folded rows this would stop paying, and the
        test should say so rather than quietly passing."""
        records = _real_records()
        postings = [r for r in records
                    if str(r.get("record_type") or "") == "context_index"]
        self.assertGreater(len(postings), 0, "no postings, so this proves nothing")
        self.assertEqual(_norm(postings), _norm(_full_fold(postings)),
                         "the cached postings are not the fold's own output any more")

    def test_a_row_that_is_not_the_folds_output_falls_back(self):
        """The policy string alone must not be enough: the fold also stamps and strips fields."""
        looks_folded = _posting(0, [1, 2])
        looks_folded.pop("index_hash")
        for rows, why in (
            ([looks_folded], "missing index_hash"),
            ([_posting(0, [1], folded=False)], "not folded at all"),
            ([_posting(3, [1]), _posting(3, [2])], "two rows in one bucket"),
            ([dict(_posting(0, [1]), node_hashes=[])], "an empty list the fold would drop"),
            ([dict(_posting(0, [1]), ref_hash=7)], "a field the fold would pop"),
            ([_posting(0, [1, 1])], "duplicate refs the fold would collapse"),
            ([_posting(0, list(range(indexing.MAX_SECONDARY_INDEX_REFS_PER_POSTING + 2)))],
             "over the ref limit, would be re-chunked"),
        ):
            self.assertIsNone(indexing._already_folded_postings([dict(r) for r in rows]),
                              "took the fast path with %s" % why)
            self.assertEqual(_norm(_full_fold(rows)),
                             _norm(indexing.compact_context_index_postings([dict(r) for r in rows])),
                             "the fallback changed the answer for %s" % why)

    def test_it_fires_on_rows_that_are_the_folds_output(self):
        """Otherwise every assertion above would pass against a fast path that never runs."""
        rows = [_posting(i, [i * 10, i * 10 + 1]) for i in range(5)]
        folded = _full_fold(rows)
        self.assertIsNotNone(indexing._already_folded_postings(list(folded)),
                             "the fast path does not recognise the fold's own output")

    def test_non_index_rows_pass_through_in_order(self):
        rows = [{"record_type": "context_event", "id": 1}, _posting(0, [1]),
                {"record_type": "skill_section", "id": 2}, _posting(1, [2])]
        folded = _full_fold(rows)
        self.assertEqual(_norm(_full_fold(folded)),
                         _norm(indexing.compact_context_index_postings(list(folded))))


if __name__ == "__main__":
    unittest.main()
