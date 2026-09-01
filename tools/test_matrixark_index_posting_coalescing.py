# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Index writes coalesce into posting lists, bounded by the same cap the compactor uses.

A skill ingest emits one index record per (chunk, term) pair: 33,020 of the 39,624 records a 1 MB
skill produces, 83.3% of them. Coalescing them into one posting per term measures, on that
document:

    records   39,624 -> 9,659    (-75.6%)
    bytes     18.4 MB -> 6.9 MB  (-62.2%)   amplification 17.5x -> 6.6x
    emit time  0.656s -> 0.428s  (-34.7%)

The index content is unchanged -- same terms, same references, different record shape.

Two things had to be true first. Serving had to resolve a multi-ref posting, which it could not:
`record_ref_hash` read every singular identity field but never `ref_hashes`, so a posting with two
refs was dropped outright. And the emitter had to respect
`MAX_SECONDARY_INDEX_REFS_PER_POSTING`, which it did not -- it wrote a single 3,302-ref posting
against a cap of 512, while `compact_context_index_postings` chunks at it. A cap that one producer
of a record type observes and another ignores is not a bound on anything.
"""
import importlib
import json
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def _emit(coalesced, chunk_count=40):
    """Emit records for a synthetic multi-section skill under one posting mode."""
    os.environ.update({
        "MATRIXARK_RESOURCE_MAX_TOTAL_CHUNKS": "500000",
        "MATRIXARK_INDEX_POSTING_LISTS": "1" if coalesced else "0",
        # These tests are about the SHAPE of a posting -- coalescing, the ref cap, the
        # singular/plural ref_hash rule -- not about which terms survive filtering. With the
        # consultable-terms filter on, a small fixture emits one source_type posting carrying
        # every chunk: no single-ref posting to assert on and the cap never approached. The
        # filter has its own tests in test_matrixark_index_consultable_terms.
        "MATRIXARK_INDEX_ONLY_CONSULTABLE_TERMS": "0",
    })
    for name in [m for m in list(sys.modules) if m.startswith("matrixark_")]:
        del sys.modules[name]
    parser = importlib.import_module("matrixark_resource_parser")
    emitter = importlib.import_module("matrixark_mcp_ingest_resource_chunk_records")
    core = importlib.import_module("matrixark_mcp_core")

    newline = chr(10)
    body = (newline * 2).join(
        ("## Section %d" % i)
        + (newline * 2)
        + ("Deploy the ingest service and verify the runbook step %d." % i)
        for i in range(chunk_count)
    )
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "_posting_fixture.md")
    with open(path, "w", encoding="utf-8") as handle:
        handle.write(body)
    try:
        chunks = parser.parse_resource(path, resource_type="skill", max_total_chunks=500000,
                                       max_chunk_tokens=128, max_chunk_chars=1024,
                                       slim_chunk_metadata_fields=True)
    finally:
        os.unlink(path)

    class Sink:
        def __init__(self):
            self.buf = []

        def append(self, record):
            self.buf.append(record)

    sink = Sink()
    emitter.append_resource_chunk_records(
        sink,
        envelope={"kind": "skill", "scope": {}, "ingestion_time_ms": 1, "metadata": {},
                  "messages": [], "resource_type": "skill", "raw_uri": path},
        parsed_chunks=chunks, chunk_vectors=[[] for _ in chunks], raw_uri=path,
        raw_uri_hash=1, resource_type="skill", resource_manifest_hash=2,
        resource_import_task_hash=3, node_hash=4, node_path=["a"], access_scope={},
        deployment_scope="local", resource_record_scope={}, skill_hash=5, skill_name="s",
        skill_metadata={}, secondary_index_budget=core.new_secondary_index_budget(500000))
    return sink.buf, core, len(chunks)


def _refs_by_term(records, core):
    """term -> set of refs, recovered through the helper the retrieve path uses."""
    out = {}
    for record in records:
        if record.get("record_type") != "context_index":
            continue
        out.setdefault(record.get("index_name"), set()).update(
            core.context_index_ref_hashes(record))
    return out


class CoalescedPostingsKeepEveryReference(unittest.TestCase):
    def test_coalescing_loses_no_reference(self):
        flat, core_flat, _ = _emit(False)
        posted, core_posted, _ = _emit(True)
        before = _refs_by_term(flat, core_flat)
        after = _refs_by_term(posted, core_posted)
        self.assertTrue(before, "no index terms emitted -- the comparison would be vacuous")
        self.assertEqual(set(before), set(after), "coalescing changed the set of index terms")
        for term, refs in before.items():
            self.assertEqual(
                refs, after[term],
                "term %r lost references: retrieval would narrow to fewer chunks" % term)

    def test_coalescing_actually_reduces_records(self):
        # Pairs with the test above: equality of references is only meaningful if the shapes
        # genuinely differ. If nothing coalesced, the equality check proves nothing.
        flat, _, _ = _emit(False)
        posted, _, _ = _emit(True)
        flat_index = [r for r in flat if r.get("record_type") == "context_index"]
        posted_index = [r for r in posted if r.get("record_type") == "context_index"]
        self.assertLess(len(posted_index), len(flat_index),
                        "coalescing produced no reduction, so the shapes are the same")

    def test_no_posting_exceeds_the_cap(self):
        # The emitter used to write one posting per term with every ref the document produced --
        # 3,302 on a real skill, against a cap of 512 the compactor observes.
        records, core, chunk_count = _emit(True, chunk_count=1400)
        cap = core.MAX_SECONDARY_INDEX_REFS_PER_POSTING
        sizes = [len(r.get("ref_hashes", []) or []) for r in records
                 if r.get("record_type") == "context_index"]
        self.assertTrue(sizes, "no postings written")
        self.assertGreater(
            max(sizes), 1,
            "no posting carried more than one ref, so the cap was never approached and this "
            "test would pass without exercising the split")
        self.assertLessEqual(max(sizes), cap,
                             "a posting exceeded MAX_SECONDARY_INDEX_REFS_PER_POSTING")

    def test_a_split_posting_is_numbered_and_drops_the_scalar(self):
        # Same singular/plural rule as compact_context_index_postings, so the two producers of
        # this record type agree on its shape.
        records, _, _ = _emit(True, chunk_count=1400)
        multi = [r for r in records
                 if r.get("record_type") == "context_index"
                 and len(r.get("ref_hashes", []) or []) > 1]
        self.assertTrue(multi, "no multi-ref posting produced")
        for record in multi:
            self.assertIn("posting_part", record)
            self.assertIsNone(record.get("ref_hash"),
                              "a multi-ref posting must not carry a singular ref_hash")

    def test_a_single_ref_posting_keeps_its_scalar(self):
        records, _, _ = _emit(True)
        singles = [r for r in records
                   if r.get("record_type") == "context_index"
                   and len(r.get("ref_hashes", []) or []) == 1]
        self.assertTrue(singles, "no single-ref posting produced")
        for record in singles:
            self.assertEqual(record["ref_hashes"][0], record.get("ref_hash"))


if __name__ == "__main__":
    unittest.main()
