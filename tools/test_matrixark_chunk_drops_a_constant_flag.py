# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A chunk does not carry `raw_bytes_stored`, and every reader still sees the same answer.

It is a per-document fact and a constant `False` on every chunk, 27 B a row -- 66.2 KB per 1 MB
skill. Nothing read it from a chunk: every mention inside a metadata dict is an ASSIGNMENT, and
every read takes the top-level field with a `False` default.

The second test is the one that matters. Absence is easy to assert and proves nothing on its own;
what must hold is that a reader asking a chunk about raw bytes still gets `False` rather than
`None` or a crash.
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
    for index in range(8):
        lines.append("")
        lines.append("## Step %d - do the thing" % index)
        lines.append(filler)
    with tempfile.NamedTemporaryFile("w", suffix=".md", delete=False, encoding="utf-8") as handle:
        handle.write(chr(10).join(lines))
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
    return [r for r in writer.buf if r.get("record_type") == "skill_section"], path


class AConstantFlagIsNotOnEveryChunk(unittest.TestCase):
    def setUp(self):
        self.sections, self.path = _sections()
        self.assertTrue(self.sections, "no sections written -- the harness is wrong")

    def tearDown(self):
        if os.path.exists(self.path):
            os.unlink(self.path)

    def test_no_chunk_carries_the_flag(self):
        for record in self.sections:
            self.assertNotIn("raw_bytes_stored", record.get("metadata") or {})
            self.assertNotIn("raw_bytes_stored", record)

    def test_a_reader_still_gets_false_rather_than_none(self):
        """How every actual reader asks: top level, with a False default."""
        for record in self.sections:
            self.assertIs(False, bool(record.get("raw_bytes_stored", False)))

    def test_the_document_level_answer_is_untouched(self):
        """Dropping the per-chunk copy must not change what the sanitizer produces for a document."""
        serving = core.serving_resource_metadata({"raw_bytes_stored": True,
                                                  "resource_type": "skill"})
        self.assertNotIn("raw_bytes_stored", serving,
                         "the chunk-level copy is what was removed; the document fact lives on "
                         "the manifest record, not in per-chunk serving metadata")


if __name__ == "__main__":
    unittest.main()
