# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""`embedding_meta` does not repeat fields the owner it rides on already carries.

The fold copies the retired embedding record wholesale minus a skip list. Three survivors --
`node_path`, `updated_at_ms`, `node_hash` -- are fields the owner has in its own right, and since
an embedding is addressed by the owner's own hash they arrive identical: 137.2 KB per 1 MB skill
restating the record they are attached to.

Two things have to hold, and the second is the one worth testing. A DIFFERING value is kept, because
"they always match" is a property of today's writers rather than a guarantee, and a mismatch is
exactly what nobody would want silently discarded. And the guard fields stay: `dim` feeds the
width-conflict check and `model` the model-hash check, which are what stop a swapped encoder from
scoring across two vector spaces.
"""
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from matrixark_mcp_local_adapter import fold_embedding_records


def _pair(*, meta_node_hash=7, meta_updated=111):
    owner = {
        "record_type": "skill_section",
        "section_hash": 42,
        "node_hash": 7,
        "node_path": ["a", "b"],
        "updated_at_ms": 111,
        "text": "body",
    }
    embedding = {
        "record_type": "context_embedding",
        "ref_type": "skill_section",
        "ref_hash": 42,
        "vector": [0.5, 0.5],
        "model": "e5-small",
        "embedding_type": "skill_section",
        "dim": 2,
        "node_hash": meta_node_hash,
        "node_path": ["a", "b"],
        "updated_at_ms": meta_updated,
    }
    return [owner, embedding]


def _folded(records):
    out = fold_embedding_records(records)
    owners = [r for r in out if r.get("record_type") == "skill_section"]
    assert owners, "the fold produced no owner -- the fixture is wrong"
    return owners[0]


class EmbeddingMetaDoesNotRestateItsOwner(unittest.TestCase):
    def test_the_vector_still_lands_on_the_owner(self):
        owner = _folded(_pair())
        self.assertEqual([0.5, 0.5], owner.get("vector"))

    def test_matching_fields_are_not_repeated(self):
        meta = _folded(_pair()).get("embedding_meta") or {}
        for key in ("node_path", "updated_at_ms", "node_hash"):
            self.assertNotIn(key, meta, "the owner already carries %s" % key)

    def test_a_differing_value_is_kept(self):
        """The case that makes the skip safe: only equal values are dropped."""
        meta = _folded(_pair(meta_node_hash=999, meta_updated=222)).get("embedding_meta") or {}
        self.assertEqual(999, meta.get("node_hash"),
                         "a node_hash that disagrees with the owner must not be discarded")
        self.assertEqual(222, meta.get("updated_at_ms"),
                         "an updated_at_ms that disagrees with the owner must not be discarded")

    def test_the_guard_fields_survive(self):
        """dim and model identify the vector space; losing them is how retrieval goes to noise."""
        meta = _folded(_pair()).get("embedding_meta") or {}
        self.assertEqual("e5-small", meta.get("model"))
        self.assertEqual(2, meta.get("dim"))
        self.assertEqual("skill_section", meta.get("embedding_type"))


if __name__ == "__main__":
    unittest.main()
