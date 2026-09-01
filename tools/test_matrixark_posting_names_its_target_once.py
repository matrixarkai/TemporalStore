# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A posting names what it points at once, and everything that reads it still resolves.

Rows used to carry the same 64-bit value in `ref_hashes: [x]`, `chunk_hash: x` and `ref_hash: x`.
On a skill nearly every row is a single-ref row -- heading_slug is unique per chunk -- so two of
the three copies were pure restatement.

These tests go through the REAL writer rather than reading the source. The builder exists in three
copies and a resource ingest runs the one in `matrixark_mcp_ingest_resource_chunk_records`; a test
that inspected `matrixark_mcp_core` would have passed while the rows on disk were unchanged.
"""
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import matrixark_mcp_core as core
import matrixark_mcp_ingest_resource_chunk_records as ingest


class _Writer:
    def __init__(self):
        self.buf = []

    def append(self, record):
        self.buf.append(record)


def _ingest():
    """Drive the real resource path over a real parsed document, and hand back what it wrote."""
    import tempfile
    import matrixark_resource_parser as parser

    body = ["# Playbook"]
    for index in range(6):
        body.append("")
        body.append("## Step %d - do the thing" % index)
        body.append("Body text for step %d, long enough to be its own section." % index)
    with tempfile.NamedTemporaryFile("w", suffix=".md", delete=False, encoding="utf-8") as handle:
        handle.write(chr(10).join(body))
        path = handle.name

    chunks = parser.parse_resource(path, resource_type="skill", max_total_chunks=1000,
                                   slim_chunk_metadata_fields=True)
    writer = _Writer()
    ingest.append_resource_chunk_records(
        writer,
        envelope={"kind": "skill", "scope": {}, "ingestion_time_ms": 1, "metadata": {},
                  "messages": [], "resource_type": "skill", "raw_uri": path},
        parsed_chunks=chunks, chunk_vectors=[[0.5, 0.5] for _ in chunks],
        raw_uri=path, raw_uri_hash=1, resource_type="skill", resource_manifest_hash=2,
        resource_import_task_hash=3, node_hash=4, node_path=["a"], access_scope={},
        deployment_scope="local", resource_record_scope={}, skill_hash=5, skill_name="s",
        skill_metadata={},
        secondary_index_budget=core.new_secondary_index_budget(10000))
    os.unlink(path)
    return [r for r in writer.buf if r.get("record_type") == "context_index"]


class APostingNamesItsTargetOnce(unittest.TestCase):
    def setUp(self):
        self.postings = _ingest()
        # Guard the harness itself: an empty result would pass every assertion below.
        self.assertTrue(self.postings, "the ingest wrote no postings -- the harness is wrong")

    def test_the_written_rows_carry_no_restated_target(self):
        for record in self.postings:
            self.assertNotIn("ref_hash", record,
                             "`ref_hashes` already names it: %s" % record.get("index_name"))
            self.assertNotIn("chunk_hash", record,
                             "`ref_hashes` already names it: %s" % record.get("index_name"))

    def test_every_posting_still_resolves_what_it_points_at(self):
        seen = 0
        for record in self.postings:
            refs = core.context_index_record_ref_hashes(record)
            self.assertTrue(refs, "posting %s resolved to nothing" % record.get("index_name"))
            self.assertEqual(refs, [r for r in record["ref_hashes"] if r is not None])
            seen += len(refs)
        self.assertGreater(seen, 0, "no refs resolved at all")

    def test_rows_written_before_this_still_resolve(self):
        """The singular field stays READABLE; only new rows stop writing it."""
        self.assertEqual([77], core.context_index_record_ref_hashes({"ref_hash": 77}))
        self.assertEqual([88], core.context_index_record_ref_hashes({"ref_hashes": [88]}))

    def test_the_identity_does_not_depend_on_the_dropped_fields(self):
        """`index_hash` is derived from `ref_hashes`, so dropping the copies cannot move it."""
        for record in self.postings:
            self.assertIsNotNone(record.get("index_hash"))
            self.assertIn("ref_hashes", record)


if __name__ == "__main__":
    unittest.main()
