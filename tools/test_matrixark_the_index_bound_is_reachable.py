# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""The secondary-index growth bound is called from the serving pipeline.

`matrixark_index_growth_bound` documents a four-lever ladder for bounding `context_index` growth,
registers two levers as operator knobs in the tenant policy with measured recall floors ("128 holds
recall 5/5 at 40 turns, 96 and below lose a fact"), and carries the comment "The bounds run on EVERY
serving recompute".

Nothing called any of them. Counted as ast.Call nodes across production files, because a grep that
merely excludes `def name` still counts strings, dict keys and comments:

                                        mentions   CALLS
    enforce_secondary_index_bounds             0       0
    dedupe_index_postings                      1       0
    sweep_index_compaction                     0       0
    max_index_records_per_scope                0       0
    index_hard_ceiling                         1       0
    node_path_embeddings_enabled  <- control   2       1
    share_serving_values_enabled  <- control   3       1

The two controls are what make that a finding rather than a broken parse: functions in the SAME
module are found when they are called, so it is specifically the index-bounding half that was dead.
Confirmed by measurement as well -- a cap sweep from 128 down to 8 changed nothing on main, while
calling the function by hand worked.

An earlier draft cited max_selected_refs (49) and top_k_per_layer (20) as the controls. They are not
callers: zero calls each, appearing only as dict keys and in a comment. They were two more instances
of the same dead code.

This is the same defect the footprint bound in the same pipeline had, in its own words: "written as
this pipeline's entry and was reachable from nowhere".

Measured with the wiring in place, 16 documents, one scope:

    cap        main (unwired)      wired
    default        46 served        46 served
    32                    -         32 served
    8              46 served         8 served

and the retrieval answer is byte-identical at every cap -- same 126 refs, same 5,226 tokens, same
fingerprint. So the default keeps today's behaviour and the knob is what changes.

These tests are about REACHABILITY. The eviction logic itself has its own suite; what was missing was
anything asserting that a serving read goes through it, which is exactly what a dead subsystem needs.

Only ONE of the three discriminates. Run against the unwired tree,
`test_a_serving_read_goes_through_the_bound` fails with "38 not less than or equal to 4"; the other
two PASS there, because a cap that does nothing trivially leaves both the view and the answer alone.
They are guards against later drift -- a default that starts evicting, an answer that starts moving --
and not evidence that the wiring works. Said here so the count is not mistaken for coverage.
"""
import tempfile
import unittest
from pathlib import Path

from tools import matrixark_mcp_local_adapter as adapter_module
from tools.matrixark_mcp_local_adapter import (
    MatrixArkLocalAdapter,
    _LOCAL_READ_CACHE,
    _LOCAL_READ_CACHE_LOCK,
)


def _clear_process_read_cache() -> None:
    with _LOCAL_READ_CACHE_LOCK:
        _LOCAL_READ_CACHE.clear()


SCOPE = {"tenant_id": "acme", "user_id": "dana", "session_id": "skills"}


def _skill_text(index: int, sections: int = 50) -> str:
    out = ["# Runbook %d" % index, ""]
    for section in range(sections):
        out += ["## Step %d" % section, "",
                "Check the queue depth for case %d step %d, then drain the backlog." % (index, section),
                ""]
    return "\n".join(out)


class IndexBoundIsReachableTest(unittest.TestCase):
    def setUp(self) -> None:
        self._dir = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(self._dir.cleanup)
        self.store = Path(self._dir.name)
        self.log = self.store / "events.jsonl"
        _clear_process_read_cache()
        self.addCleanup(_clear_process_read_cache)

    def _ingest(self, documents: int = 12) -> None:
        for index in range(documents):
            adapter = MatrixArkLocalAdapter(self.log)
            adapter.ingest({"kind": "skill", "scope": SCOPE, "text": _skill_text(index),
                            "metadata": {"raw_uri": "file:///s/d-%d.md" % index,
                                         "title": "d-%d" % index}})
            adapter.close(timeout_s=3600)
        _clear_process_read_cache()

    def _served_postings(self) -> int:
        _clear_process_read_cache()
        return sum(1 for record in MatrixArkLocalAdapter(self.log).read_all()
                   if str(record.get("record_type") or "") == "context_index")

    def _with_cap(self, cap: int):
        """Set the cap the way an operator would, through the policy accessor's environment."""
        import os
        for name, value in (("MATRIXARK_MAX_SECONDARY_INDEX_RECORDS_PER_SESSION", str(cap)),
                            ("MATRIXARK_MAX_SECONDARY_INDEX_RECORDS_PER_TENANT", str(cap * 8))):
            previous = os.environ.get(name)
            os.environ[name] = value
            self.addCleanup(
                (lambda n, p: (os.environ.__setitem__(n, p) if p is not None
                               else os.environ.pop(n, None))), name, previous)

    def test_a_serving_read_goes_through_the_bound(self):
        """The whole finding in one assertion: on main this number does not move.

        A cap of 4 is far below anything an operator would run, and that is the point -- it makes
        the difference between "called" and "not called" impossible to miss.
        """
        self._ingest()
        unbounded = self._served_postings()
        self.assertGreater(unbounded, 8,
                           "not enough postings to bound; this would pass without the wiring")

        self._with_cap(4)
        bounded = self._served_postings()
        self.assertLessEqual(bounded, 4,
                             "the serving read did not go through enforce_secondary_index_bounds; "
                             "the operator knob is doing nothing, which is how it shipped")
        self.assertLess(bounded, unbounded)

    def test_the_default_leaves_the_view_alone(self):
        """Wiring a dormant bound must not change what a store below the cap serves. The bound
        returns its input unchanged when nothing is over budget, and this pins that."""
        self._ingest()
        default_view = MatrixArkLocalAdapter(self.log).read_all()
        _clear_process_read_cache()
        self._with_cap(100000)
        self.assertEqual(default_view, MatrixArkLocalAdapter(self.log).read_all(),
                         "a cap nothing reaches still changed the served view")

    def test_bounding_the_postings_does_not_change_the_answer_here(self):
        """Measured, and stated as a property of THIS corpus rather than a general claim: at 16
        documents in one scope the answer is byte-identical from 46 postings down to 8. The module's
        own docs are explicit that evictions CAN cost recall on a conversational corpus, which is a
        different workload and has its own measured floor of 128."""
        self._ingest()
        query = {"scope": SCOPE, "query": "drain the backlog when the queue depth grows", "limit": 8}

        _clear_process_read_cache()
        before = MatrixArkLocalAdapter(self.log).retrieve(dict(query))
        self._with_cap(8)
        _clear_process_read_cache()
        after = MatrixArkLocalAdapter(self.log).retrieve(dict(query))

        def keys(pack):
            return [str(item.get("ref_hash") or item) if isinstance(item, dict) else str(item)
                    for item in (pack.get("selected_refs") or [])]

        self.assertTrue(keys(before), "the baseline selected nothing, so this compares nothing")
        self.assertEqual(keys(before), keys(after))
        self.assertEqual(before.get("used_context_tokens"), after.get("used_context_tokens"))


if __name__ == "__main__":
    unittest.main()
