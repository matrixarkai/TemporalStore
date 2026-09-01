# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A posting is not written when the record it points at can work the term out for itself.

Since the fold, a chunk carries its own vector and the separate embedding record is dropped. That
is the exact condition the secondary prefilter's owner branch is gated on::

    if not owner_record.get("vector") and not owner_record.get("embedding_meta"): continue

Before the fold that branch never ran for chunks, so a posting was the only way to reach one.
Now the owner is always there, and `candidate_index_terms` recomputes the same terms from it.

The two tests that matter are the negative ones. A skip that fired unconditionally would empty the
index and make those queries match nothing -- silently, which is the failure this whole area keeps
producing. So: terms the owner CANNOT derive must still be written, and a chunk with no vector must
keep every posting it had.
"""
import collections
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


def _ingest(*, skill_metadata=None, with_vectors=True):
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
    try:
        chunks = parser.parse_resource(path, resource_type="skill", max_total_chunks=1000,
                                       slim_chunk_metadata_fields=True)
        vectors = [[0.5, 0.5] if with_vectors else [] for _ in chunks]
        writer = _Writer()
        ingest.append_resource_chunk_records(
            writer,
            envelope={"kind": "skill", "scope": {}, "ingestion_time_ms": 1, "metadata": {},
                      "messages": [], "resource_type": "skill", "raw_uri": path},
            parsed_chunks=chunks, chunk_vectors=vectors, raw_uri=path, raw_uri_hash=1,
            resource_type="skill", resource_manifest_hash=2, resource_import_task_hash=3,
            node_hash=4, node_path=["a"], access_scope={}, deployment_scope="local",
            resource_record_scope={}, skill_hash=5, skill_name="acme",
            skill_metadata=skill_metadata or {},
            secondary_index_budget=core.new_secondary_index_budget(10000))
        return writer.buf
    finally:
        os.unlink(path)


def _kinds(records):
    return collections.Counter(
        str(r.get("index_name", "")).partition(":")[0]
        for r in records if r.get("record_type") == "context_index")


class APostingIsNotWrittenForWhatTheOwnerKnows(unittest.TestCase):
    def test_the_owner_really_can_derive_the_terms_that_are_skipped(self):
        """The premise, checked directly rather than assumed."""
        records = _ingest()
        owners = [r for r in records if r.get("record_type") == "skill_section"]
        self.assertTrue(owners, "no owners written -- the harness is wrong")
        for owner in owners:
            derived = core.candidate_index_terms(owner, {}, {})
            self.assertTrue([t for t in derived if t.startswith("heading_slug:")],
                            "the owner cannot derive its own heading_slug, so skipping the "
                            "posting for it would lose the term")

    def test_derivable_terms_are_not_written_as_postings(self):
        kinds = _kinds(_ingest())
        for kind in ("heading_slug", "unit_kind", "resource_type", "source_type"):
            self.assertNotIn(kind, kinds,
                             "%s is derivable from the owner; the posting restates it" % kind)

    def test_terms_the_owner_cannot_derive_are_still_written(self):
        """skill_trigger and skill_tool come from the skill manifest, not the chunk."""
        kinds = _kinds(_ingest(skill_metadata={"triggers": ["refund_flow"],
                                               "allowed_tools": ["matrixark_ingest"]}))
        self.assertIn("skill_trigger", kinds,
                      "a term no chunk can derive must still reach the index")
        self.assertIn("skill_tool", kinds)

    def test_a_chunk_with_no_vector_keeps_every_posting(self):
        """Without a vector the prefilter skips the owner, so the posting is the only route."""
        kinds = _kinds(_ingest(skill_metadata={"triggers": ["refund_flow"]}, with_vectors=False))
        self.assertIn("heading_slug", kinds,
                      "an owner the prefilter will skip must keep its postings")
        for kind in ("unit_kind", "resource_type", "source_type"):
            self.assertIn(kind, kinds)

    def test_the_escape_hatch_restores_every_posting(self):
        import importlib
        os.environ["MATRIXARK_INDEX_SKIP_OWNER_DERIVABLE_TERMS"] = "0"
        try:
            sys.modules.pop("matrixark_mcp_ingest_resource_chunk_records", None)
            module = importlib.import_module("matrixark_mcp_ingest_resource_chunk_records")
            self.assertFalse(module.INDEX_SKIP_OWNER_DERIVABLE_TERMS)
        finally:
            os.environ.pop("MATRIXARK_INDEX_SKIP_OWNER_DERIVABLE_TERMS", None)
            sys.modules.pop("matrixark_mcp_ingest_resource_chunk_records", None)
            importlib.import_module("matrixark_mcp_ingest_resource_chunk_records")


if __name__ == "__main__":
    unittest.main()
