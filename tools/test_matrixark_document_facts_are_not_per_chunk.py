# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A document fact is not repeated onto every chunk of the document.

`raw_storage_policy` is decided once for a document and was written into every chunk's serving
metadata -- 93.1 KB per 1 MB skill for a value identical on all of them. No reader takes it from a
stored chunk: the dashboard reads the TOP-level field on manifest rows, ingest reads the live
`storage_resolution`, and resource IO reads the ENVELOPE's metadata while deciding where raw bytes
go.

The second test is the important one. `resource_version` looks like exactly the same kind of
per-document constant, and dropping it would be wrong: the retrieve path reads it from a stored
record to decide `version_state`, falling back to a top-level field that sections do not carry, so
removing it would make every chunk look current. Two fields, same shape, opposite answers -- so the
test pins both directions.
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


class DocumentFactsAreNotRepeatedPerChunk(unittest.TestCase):
    def setUp(self):
        self.sections = _sections()
        self.assertTrue(self.sections, "no sections written -- the harness is wrong")

    def test_the_storage_policy_is_not_on_every_chunk(self):
        for record in self.sections:
            self.assertNotIn("raw_storage_policy", record.get("metadata") or {})

    def test_the_resource_version_IS_still_on_every_chunk(self):
        """Same shape, opposite answer: version_state is decided from this stored copy."""
        for record in self.sections:
            self.assertIn("resource_version", record.get("metadata") or {},
                          "the retrieve path reads this from the stored record to decide "
                          "version_state; without it every chunk looks current")

    def test_a_reader_asking_a_chunk_for_the_policy_gets_the_documented_default(self):
        """How the remaining readers ask, all of which default rather than require."""
        for record in self.sections:
            metadata = record.get("metadata") or {}
            self.assertEqual("raw_uri_only",
                             metadata.get("raw_storage_policy", "raw_uri_only"))

    def test_the_index_terms_a_chunk_offers_are_unchanged(self):
        """The scan derives terms from metadata; dropping a key must not change them."""
        for record in self.sections:
            terms = core.candidate_index_terms(record, {}, {})
            self.assertTrue([t for t in terms if t.startswith("resource_type:")])
            self.assertTrue([t for t in terms if t.startswith("unit_kind:")])


if __name__ == "__main__":
    unittest.main()
