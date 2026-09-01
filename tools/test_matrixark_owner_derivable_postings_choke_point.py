# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Postings the owner can derive are dropped once, for every writer.

`context_index_posting_record` has fourteen call sites across five modules. Editing one moved a
100-event store's index by 0.1 KB of 120.2, because a normal ingest takes a different branch than
the one edited. So the decision is made where every write already passes: beside the fold, which
already resolves owners.

The negative cases are the ones worth testing. A skip that fired unconditionally would delete
postings nothing can replace, and that failure is silent -- the query simply matches nothing.
"""
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import matrixark_mcp_core as core
from matrixark_mcp_local_adapter import drop_owner_derivable_postings


def _owner(**over):
    record = {
        "record_type": "context_event",
        "event_id_hash": 42,
        "event_type": "status_update",
        "classification": "operational",
        "status": "observed",
        "source_role": "assistant",
        "text": "restarted the worker",
        "vector": [0.5, 0.5],
    }
    record.update(over)
    return record


def _posting(name, refs=(42,), ref_type="event"):
    return {"record_type": "context_index", "index_name": name,
            "ref_type": ref_type, "ref_hashes": list(refs)}


def _derivable_term(owner):
    terms = sorted(core.candidate_index_terms(owner, {}, {}))
    assert terms, "the fixture owner derives no terms"
    return terms[0]


def _reload_flag_modules():
    """Drop EVERY spelling of the flag's module, not just the flat one.

    tools/ is importable as `x` and as `tools.x`; python keeps those as separate modules with
    separate module-level flags. Popping one and reloading leaves the other stale, which makes an
    environment override look ignored.
    """
    import importlib
    for name in [m for m in list(sys.modules)
                 if m.endswith("matrixark_mcp_ingest_resource_chunk_records")
                 or m.endswith("matrixark_mcp_local_adapter")
                 or m.endswith("matrixark_local_adapter_retrieval")
                 or m.endswith("matrixark_resource_parser")]:
        sys.modules.pop(name, None)
    return importlib


class PostingsTheOwnerCanDeriveAreDropped(unittest.TestCase):
    def test_a_derivable_posting_is_dropped(self):
        owner = _owner()
        term = _derivable_term(owner)
        out = drop_owner_derivable_postings([owner, _posting(term)])
        self.assertEqual([owner], out, "the owner derives %s for itself" % term)

    def test_a_term_the_owner_cannot_derive_survives(self):
        owner = _owner()
        out = drop_owner_derivable_postings([owner, _posting("skill_trigger:refund_flow")])
        self.assertEqual(2, len(out), "a term no owner can derive must keep its posting")

    def test_an_owner_without_a_vector_keeps_its_postings(self):
        """The prefilter skips such an owner, so the posting is the only route to it."""
        owner = _owner()
        owner.pop("vector")
        term = _derivable_term(owner)
        out = drop_owner_derivable_postings([owner, _posting(term)])
        self.assertEqual(2, len(out))

    def test_an_unresolvable_owner_keeps_its_posting(self):
        out = drop_owner_derivable_postings([_posting("event_type:status_update", refs=(999,))])
        self.assertEqual(1, len(out), "no owner in the batch and none resolvable")

    def test_a_multi_ref_posting_needs_every_owner_to_cover_it(self):
        """One uncovered ref keeps the whole row -- dropping it would strand that ref."""
        covered = _owner()
        term = _derivable_term(covered)
        out = drop_owner_derivable_postings([covered, _posting(term, refs=(42, 4242))])
        self.assertEqual(2, len(out), "ref 4242 has no owner here, so the row must stay")

    def test_a_ref_type_with_no_owner_mapping_is_untouched(self):
        out = drop_owner_derivable_postings(
            [_posting("entity_type:person", ref_type="batch_commit")])
        self.assertEqual(1, len(out))

    def test_the_escape_hatch_keeps_everything(self):
        os.environ["MATRIXARK_INDEX_SKIP_OWNER_DERIVABLE_TERMS"] = "0"
        try:
            importlib = _reload_flag_modules()
            reloaded = importlib.import_module("matrixark_mcp_local_adapter")
            owner = _owner()
            out = reloaded.drop_owner_derivable_postings(
                [owner, _posting(_derivable_term(owner))])
            self.assertEqual(2, len(out))
        finally:
            os.environ.pop("MATRIXARK_INDEX_SKIP_OWNER_DERIVABLE_TERMS", None)
            _reload_flag_modules()
            importlib.import_module("matrixark_mcp_local_adapter")


if __name__ == "__main__":
    unittest.main()
