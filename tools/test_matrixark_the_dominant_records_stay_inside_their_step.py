# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""The two record types that ARE a skill corpus must stay inside their dict step.

A 1.00 MB skill produces ~2,120 records, and `skill_section` and `resource_chunk` are 99.1% of them.
Both currently hold exactly 21 keys -- and a python dict does not grow smoothly. Its table is sized
in steps:

    keys    1-5    6-10   11-21   22-42   43-85
    bytes   232    360    640     1176    2272

21 is the LAST slot in the 640 B step. Adding one key to either type moves every one of them to
1,176 B: +536 B on 99.1% of 2.35M records, about **+1.26 GB** at 1,000 x 1 MB.

So a field added here is not priced at its own width. A three-byte flag costs the same as a fat
blob, because what it consumes is a SLOT. This test makes that cost visible at the moment it would
be incurred, rather than in a footprint measurement months later.

It is deliberately a CEILING, not an equality: dropping keys is welcome and must not fail. If a field
genuinely has to be added, the honest move is to remove another, or to change this bound with the
1.26 GB written down beside it.
"""
import collections
import tempfile
import unittest
from pathlib import Path

from tools.matrixark_mcp_local_adapter import (
    MatrixArkLocalAdapter,
    _LOCAL_READ_CACHE,
    _LOCAL_READ_CACHE_LOCK,
)

SCOPE = {"tenant_id": "acme", "user_id": "dana", "session_id": "skills"}

#: The last key count that still fits the 640 B dict step. See the module docstring.
KEYS_IN_THE_640_BYTE_STEP = 21

#: The types that dominate a skill corpus, and so are the only ones where a step matters.
DOMINANT = ("skill_section", "resource_chunk")


def _clear_process_read_cache() -> None:
    with _LOCAL_READ_CACHE_LOCK:
        _LOCAL_READ_CACHE.clear()


def _skill_text(index: int, sections: int = 220) -> str:
    out = ["# Runbook %d" % index, ""]
    for section in range(sections):
        out += ["## Step %d" % section, "",
                "Check the queue depth for case %d step %d, then drain the backlog and confirm "
                "the worker restarted cleanly." % (index, section), ""]
    return "\n".join(out)


class DominantRecordsStayInsideTheirStepTest(unittest.TestCase):
    def setUp(self) -> None:
        self._dir = tempfile.TemporaryDirectory()
        self.addCleanup(self._dir.cleanup)
        self.log = Path(self._dir.name) / "events.jsonl"
        _clear_process_read_cache()
        self.addCleanup(_clear_process_read_cache)
        self.records = self._ingest()

    def _ingest(self, documents: int = 2):
        for index in range(documents):
            adapter = MatrixArkLocalAdapter(self.log)
            adapter.ingest({
                "kind": "skill", "scope": SCOPE, "text": _skill_text(index),
                "metadata": {"raw_uri": "file:///s/doc-%d.md" % index, "title": "doc-%d" % index},
            })
            adapter.close(timeout_s=3600)
        _clear_process_read_cache()
        return MatrixArkLocalAdapter(self.log).read_all()

    def test_these_two_types_really_are_the_corpus(self):
        """The premise. If they stop dominating, the bound below stops being the thing that matters.

        Asserted as "the two most numerous types", which holds at any corpus size. The 99.1% figure
        is measured at a full 1.00 MB document, where the fixed per-document records are amortised;
        at the smaller size this test ingests they are a larger fraction, so a percentage threshold
        here would be pinning the test's own scale rather than the property.
        """
        counts = collections.Counter(str(r.get("record_type") or "?") for r in self.records)
        self.assertGreater(len(self.records), 150, "corpus too small to rank types")
        top_two = {name for name, _ in counts.most_common(2)}
        self.assertEqual(
            set(DOMINANT), top_two,
            "the two most numerous types are now %s, not %s; re-check which types carry the "
            "footprint before trusting the bound below" % (sorted(top_two), sorted(DOMINANT)))

    def test_no_dominant_record_leaves_the_640_byte_step(self):
        for name in DOMINANT:
            rows = [r for r in self.records if str(r.get("record_type") or "") == name]
            self.assertTrue(rows, "no %s records were written, so this bound is untested" % name)
            widest = max(len(row) for row in rows)
            self.assertLessEqual(
                widest, KEYS_IN_THE_640_BYTE_STEP,
                "%s now holds %d keys. Past %d its dict table doubles to 1,176 B, which is "
                "+536 B on ~99%% of 2.35M records -- about +1.26 GB at 1,000 x 1 MB. Remove a "
                "key, or raise this bound deliberately with that cost recorded."
                % (name, widest, KEYS_IN_THE_640_BYTE_STEP))

    def test_the_bound_is_the_step_boundary_not_an_arbitrary_number(self):
        """Pin the reason, so the constant cannot be nudged as if it were a style choice."""
        at_bound = dict.fromkeys(range(KEYS_IN_THE_640_BYTE_STEP))
        past_bound = dict.fromkeys(range(KEYS_IN_THE_640_BYTE_STEP + 1))
        import sys
        self.assertLess(
            sys.getsizeof(at_bound), sys.getsizeof(past_bound),
            "on this interpreter %d and %d keys cost the same, so the boundary moved -- "
            "re-derive it before trusting the bound"
            % (KEYS_IN_THE_640_BYTE_STEP, KEYS_IN_THE_640_BYTE_STEP + 1))


if __name__ == "__main__":
    unittest.main()
