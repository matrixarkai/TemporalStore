# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A chunk carries its heading once, and still carries the slug the index is built from.

`heading` was stored twice on every chunk -- once at the top level, once inside `metadata` -- and
the two were byte-identical on all 2,510 chunks of a 1 MB skill. The copy is gone.

`heading_slug` LOOKS like the better candidate for removal, since it is slugify(heading) and the
heading is right there. It is not. It is the only source of the `heading_slug:` index terms: drop
it and `candidate_index_terms` derives nothing for that kind, so a query narrowing on a heading
anchor intersects against a term no candidate can offer and matches nothing. Both halves are
pinned here so the pair cannot be swapped by someone reading only the first line of the reasoning.
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
    for index in range(10):
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
    os.unlink(path)
    return writer.buf


class AHeadingIsStoredOnce(unittest.TestCase):
    def setUp(self):
        self.records = _sections()
        self.sections = [r for r in self.records
                         if r.get("record_type") == "skill_section" and r.get("heading")]
        self.assertTrue(self.sections, "no sections with a heading -- the harness is wrong")

    def test_the_metadata_no_longer_repeats_the_heading(self):
        for record in self.sections:
            self.assertNotIn("heading", record.get("metadata") or {},
                             "the row already carries `heading` at the top level")
            self.assertTrue(record.get("heading"), "the top-level heading must survive")

    def test_the_slug_survives_and_still_yields_its_index_terms(self):
        for record in self.sections:
            self.assertIn("heading_slug", record.get("metadata") or {},
                          "heading_slug is the only source of heading_slug: index terms")
            terms = core.candidate_index_terms(record, {}, {})
            self.assertTrue(
                [t for t in terms if t.startswith("heading_slug:")],
                "no heading_slug term derivable, so a heading-anchored query matches nothing")

    def test_the_heading_slug_term_is_still_reachable(self):
        """Reachability is the invariant; a posting was only ever one way to provide it.

        Chunks now carry their own vector, so the prefilter reaches the owner and recomputes the
        term from it, and the posting that restated it is no longer written. What must stay true
        is that some route still offers a `heading_slug:` term for every section -- whether that
        is a stored row or the owner deriving it.
        """
        for record in self.sections:
            posted = [r for r in self.records
                      if str(r.get("index_name", "")).startswith("heading_slug:")]
            derived = [t for t in core.candidate_index_terms(record, {}, {})
                       if t.startswith("heading_slug:")]
            self.assertTrue(posted or derived,
                            "no route offers a heading_slug term for this section")


if __name__ == "__main__":
    unittest.main()
