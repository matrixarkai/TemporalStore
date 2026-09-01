# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""`embedding_meta` does not repeat the owner's record_type under another name.

For a chunk the fold's owner map is identity -- `skill_section -> skill_section`,
`resource_chunk -> resource_chunk` -- so `embedding_type` arrives equal to the owner's
`record_type`, measured equal on 2510/2510 chunks of a 1 MB skill. 85.8 KB restating it.

The map is NOT identity for the other six owner types: `event -> context_event`,
`summary -> context_summary`, and so on. There the value says something the owner cannot --
`node_l0` is not `context_summary` -- so it is compared rather than assumed, and a differing value
is kept. That mismatch case is the test that matters.
"""
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from matrixark_mcp_local_adapter import fold_embedding_records


def _folded(owner_type, embedding_type, id_field, ref_type):
    owner = {"record_type": owner_type, id_field: 42, "text": "body"}
    embedding = {
        "record_type": "context_embedding",
        "ref_type": ref_type,
        "ref_hash": 42,
        "vector": [0.5, 0.5],
        "model": "e5-large",
        "embedding_type": embedding_type,
        "dim": 2,
    }
    out = fold_embedding_records([owner, embedding])
    owners = [r for r in out if r.get("record_type") == owner_type]
    assert owners, "the fold produced no owner -- the fixture is wrong"
    return owners[0]


class EmbeddingTypeIsNotRepeated(unittest.TestCase):
    def test_a_chunk_does_not_repeat_its_own_record_type(self):
        for owner_type, id_field, ref_type in (("skill_section", "section_hash", "skill_section"),
                                               ("resource_chunk", "chunk_hash", "resource_chunk")):
            meta = _folded(owner_type, owner_type, id_field, ref_type).get("embedding_meta") or {}
            self.assertNotIn("embedding_type", meta,
                             "%s already says what it is" % owner_type)

    def test_a_differing_embedding_type_is_kept(self):
        """The case that makes the drop safe: summary owners carry node_l0, not context_summary."""
        meta = _folded("context_summary", "node_l0", "summary_hash", "summary").get("embedding_meta") or {}
        self.assertEqual("node_l0", meta.get("embedding_type"),
                         "node_l0 is not the owner's record_type and must survive")

    def test_the_guard_fields_survive(self):
        """dim and model identify the vector space; losing them is how retrieval goes to noise."""
        meta = _folded("skill_section", "skill_section", "section_hash", "skill_section").get("embedding_meta") or {}
        self.assertEqual("e5-large", meta.get("model"))
        self.assertEqual(2, meta.get("dim"))

    def test_the_vector_still_lands_on_the_owner(self):
        owner = _folded("skill_section", "skill_section", "section_hash", "skill_section")
        self.assertEqual([0.5, 0.5], owner.get("vector"))


if __name__ == "__main__":
    unittest.main()
