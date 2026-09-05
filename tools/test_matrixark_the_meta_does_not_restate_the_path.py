# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""`embedding_meta` must not restate the owning record's path.

`node_path` sat in `_EMBEDDING_META_SAME_AS_OWNER`, which drops a field only when the OWNER carries
the same value. The two record types that are 99.1% of a skill corpus carry `node_hash` and no
`node_path`, so that comparison never ran and the path was written on every record -- 54.9 B a row,
one distinct value across the whole corpus, about 129 MB of durable bytes at 1,000 x 1 MB.

Nothing reads it. A recording dict over every `embedding_meta`, driven through `read_all` and a
retrieve, was asked for `node_path` zero times across 4,326 records while `model` was read 4,324
times; no Python reader exists outside a comment; and the `node_path` matches in the Rust crates
belong to an unrelated Raft file-path helper.

Every assertion here is paired with one that would fail if the meta were simply empty -- a test that
"the field is absent" passes loudest when there is nothing to be absent from.
"""
import tempfile
import unittest
from pathlib import Path

from tools.matrixark_mcp_local_adapter import (
    _EMBEDDING_META_SAME_AS_OWNER,
    _EMBEDDING_META_SKIP,
    MatrixArkLocalAdapter,
    _LOCAL_READ_CACHE,
    _LOCAL_READ_CACHE_LOCK,
)

SCOPE = {"tenant_id": "acme", "user_id": "dana", "session_id": "skills"}
DOMINANT = ("skill_section", "resource_chunk")


def _clear_process_read_cache() -> None:
    with _LOCAL_READ_CACHE_LOCK:
        _LOCAL_READ_CACHE.clear()


def _skill_text(index: int, sections: int = 40) -> str:
    out = ["# Runbook %d" % index, ""]
    for section in range(sections):
        out += ["## Step %d" % section, "",
                "Check the queue depth for case %d step %d, then drain the backlog." % (index, section),
                ""]
    return "\n".join(out)


class MetaDoesNotRestateThePathTest(unittest.TestCase):
    def setUp(self) -> None:
        self._dir = tempfile.TemporaryDirectory()
        self.addCleanup(self._dir.cleanup)
        self.log = Path(self._dir.name) / "events.jsonl"
        _clear_process_read_cache()
        self.addCleanup(_clear_process_read_cache)

    def _records(self, documents: int = 2):
        for index in range(documents):
            adapter = MatrixArkLocalAdapter(self.log)
            adapter.ingest({
                "kind": "skill", "scope": SCOPE, "text": _skill_text(index),
                "metadata": {"raw_uri": "file:///s/doc-%d.md" % index, "title": "doc-%d" % index},
            })
            adapter.close(timeout_s=3600)
        _clear_process_read_cache()
        return MatrixArkLocalAdapter(self.log).read_all()

    def test_the_meta_carries_no_node_path(self):
        records = self._records()
        metas = [r["embedding_meta"] for r in records if isinstance(r.get("embedding_meta"), dict)]

        # Non-vacuity: there must BE metas, and they must still carry the field that is read.
        self.assertTrue(metas, "no embedding_meta was written, so absence proves nothing")
        self.assertTrue(all("model" in meta for meta in metas),
                        "embedding_meta lost `model`, which the retrieve path reads")

        carrying = [meta for meta in metas if "node_path" in meta]
        self.assertEqual([], carrying,
                         "%d embedding_meta dict(s) still restate the owner's node_path"
                         % len(carrying))

    def test_the_owner_still_has_no_node_path_to_compare_against(self):
        """The reason the old rule could not drop it, pinned.

        If a future change gives these record types their own `node_path`, the
        `_EMBEDDING_META_SAME_AS_OWNER` rule would start firing and this removal would become
        redundant rather than wrong -- but the premise should be stated, not assumed.
        """
        records = self._records()
        dominant = [r for r in records if str(r.get("record_type") or "") in DOMINANT]
        self.assertTrue(dominant, "no dominant records were written")
        self.assertTrue(all("node_hash" in record for record in dominant),
                        "the dominant records lost node_hash, so this test is measuring something else")
        with_path = [r for r in dominant if "node_path" in r]
        self.assertEqual([], with_path,
                         "the owner now carries node_path; revisit whether the skip is still needed")

    def test_the_skip_list_owns_it_and_the_match_rule_does_not(self):
        """Where the field is dropped matters: the match rule could never reach it."""
        self.assertIn("node_path", _EMBEDDING_META_SKIP)
        self.assertNotIn("node_path", _EMBEDDING_META_SAME_AS_OWNER)
        # The rule still has work to do for the fields the owner really does carry.
        self.assertIn("node_hash", _EMBEDDING_META_SAME_AS_OWNER)
        self.assertIn("updated_at_ms", _EMBEDDING_META_SAME_AS_OWNER)

    def test_the_model_identity_fields_are_kept(self):
        """`model` and `model_ref` scan just as clean and are deliberately NOT removed: a mis-set
        model path falls back to a different vector dimension, and the model identity on an
        embedding is what makes that detectable."""
        self.assertNotIn("model", _EMBEDDING_META_SKIP)
        self.assertNotIn("model_ref", _EMBEDDING_META_SKIP)
        records = self._records()
        metas = [r["embedding_meta"] for r in records if isinstance(r.get("embedding_meta"), dict)]
        self.assertTrue(metas)
        self.assertTrue(all("model" in meta for meta in metas))


if __name__ == "__main__":
    unittest.main()
