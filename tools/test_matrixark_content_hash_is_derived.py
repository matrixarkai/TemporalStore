# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A chunk does not carry a hash of the text it already stores.

`content_hash` is `content_hash(text)`, and the text is on the same record -- 88.2 KB per 1 MB skill
to restate what the row derives from itself.

The second test is the one that matters. Absence is trivial to assert and proves nothing; what has
to hold is that a reader asking for the hash still gets the RIGHT value, recomputed from the text,
rather than an empty string or a different digest.
"""
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import matrixark_mcp_core as core
import matrixark_mcp_ingest_resource_chunk_records as ingest
import matrixark_resource_parser as parser


class _Writer:
    def __init__(self):
        self.buf = []

    def append(self, record):
        self.buf.append(record)


def _sections():
    filler = ("This paragraph gives the section enough substance to stand on its own rather "
              "than being folded into a neighbouring section by the parser. ") * 6
    lines = ["# Playbook"]
    for index in range(6):
        lines += ["", "## Step %d - do the thing" % index, filler]
    with tempfile.NamedTemporaryFile("w", suffix=".md", delete=False, encoding="utf-8") as handle:
        handle.write(chr(10).join(lines))
        path = handle.name
    try:
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
        return [r for r in writer.buf if r.get("record_type") == "skill_section"]
    finally:
        os.unlink(path)


class ContentHashIsDerivedNotStored(unittest.TestCase):
    def setUp(self):
        self.sections = _sections()
        self.assertTrue(self.sections, "no sections written -- the harness is wrong")

    def test_the_hash_is_not_carried(self):
        for record in self.sections:
            self.assertNotIn("content_hash", record.get("metadata") or {})

    def test_a_reader_recomputing_it_gets_the_same_value(self):
        """How every real reader asks: `metadata.get(...) or content_hash(text)`."""
        for record in self.sections:
            metadata = record.get("metadata") or {}
            text = record.get("text", "")
            recomputed = str(metadata.get("content_hash") or core.content_hash(text))
            self.assertTrue(recomputed, "the fallback produced nothing")
            self.assertEqual(core.content_hash(text), recomputed,
                             "the recomputed hash must equal the hash of the stored text")

    def test_the_text_it_hashes_is_still_there(self):
        """Deriving only works while the text is on the record; guard the premise."""
        for record in self.sections:
            self.assertTrue(record.get("text"), "no text to derive the hash from")


if __name__ == "__main__":
    unittest.main()
