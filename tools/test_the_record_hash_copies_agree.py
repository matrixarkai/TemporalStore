"""`record_primary_hash` exists twice and the copies agree. Pinned, and NOT consolidated.

`matrixark_mcp_direct_cache.record_primary_hash` and
`_TemporalDirectReadMixin._record_primary_hash` carry the same body. The field order in it is a
record's identity PRECEDENCE -- which of `event_id_hash`, `entity_hash`, `segment_hash` and the rest
is consulted first decides which hash identifies a record. Two copies of that order are two chances
to disagree about identity, and a disagreement would not raise. It would file a record under a
different hash.

## Why this pins them instead of merging them

Adopting the free function looked right and was tried. It is not, and the reason is worth keeping:

* `matrixark_mcp_direct_cache` is recorded as a module **production cannot reach**. Wiring one
  function up brings the whole module into the reachability scan, which then reports **nine**
  unreached definitions inside it -- `direct_context_pack_response_cache_get/_key/_put`,
  `ensure_direct_context_pack_response_cache`, `direct_record_load_lock`,
  `placement_candidate_records_from_cache_or_load`, `placement_candidate_table_cache_key`,
  `prune_retrieval_candidate_cache`, `retrieval_candidate_cache_key`. The module is not a helper
  library with one stray function; it is a parallel implementation of caching that
  `matrixark_temporal_direct_read` provides as methods, and nothing uses it.
* One of those, `prune_retrieval_candidate_cache`, would be an outright **bug** to adopt. The two
  modules hold SEPARATE cache dicts -- `direct_cache._DIRECT_RETRIEVAL_CANDIDATE_CACHE` is not the
  same object as the reader's -- so each prunes its own. Pointing the live method at this copy
  would prune a cache nothing populates and leave the live one growing. The two bodies are
  identical and touch the same six names; only RUNNING them showed it.

So the duplication stays, and what this file does is make it unable to drift silently. Deleting the
dead module, or moving the shared body somewhere live, is a larger change than a test should make.

Asserted by ANSWER, not by object identity: two names resolving is half the promise, and comparing
the bodies would not catch a copy whose behaviour moved.
"""

import pathlib
import sys
import unittest

TOOLS = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(TOOLS))

# matrixark_temporal_direct_read and matrixark_mcp_temporal_adapters import each other, so importing
# the reader first raises from a partially initialized module. Import the cycle parent first -- not
# a sys.path change, which is process-wide and would alter how other tests resolve their imports.
import matrixark_mcp_temporal_adapters  # noqa: F401
import matrixark_mcp_direct_cache
import matrixark_temporal_direct_read

FIELDS = (
    "event_id_hash", "entity_hash", "segment_hash", "compression_id_hash", "summary_hash",
    "chunk_hash", "section_hash", "skill_hash", "resource_hash", "batch_id_hash", "ref_hash",
)


def _probes():
    out = [{}, {"ref": "x"}, {"record_type": "context_event"}]
    for field in FIELDS:
        out.extend([{field: 1234}, {field: "abc"}, {field: None}, {field: 0}])
    for i in range(len(FIELDS)):
        for j in range(i + 1, min(i + 4, len(FIELDS))):
            out.append({FIELDS[i]: 11, FIELDS[j]: 22})   # precedence, not just presence
    return out


class _Fake:
    """The method reads nothing off self, which is why a free copy of it exists at all."""


class RecordPrimaryHashCopiesAgreeTest(unittest.TestCase):

    def test_the_probe_set_exercises_precedence(self):
        probes = _probes()
        self.assertGreater(len(probes), 50, "only %d probes" % len(probes))
        multi = [p for p in probes if len(p) > 1]
        self.assertGreater(len(multi), 10,
                           "only %d probes carry two fields, so PRECEDENCE is barely exercised "
                           "and agreement would mostly prove presence" % len(multi))

    def test_the_two_copies_answer_identically(self):
        free = matrixark_mcp_direct_cache.record_primary_hash
        method = matrixark_temporal_direct_read._TemporalDirectReadMixin._record_primary_hash
        fake = _Fake()
        disagreements = []
        for probe in _probes():
            try:
                a = free(dict(probe))
            except Exception as exc:
                a = "raised %s" % type(exc).__name__
            try:
                b = method(fake, dict(probe))
            except Exception as exc:
                b = "raised %s" % type(exc).__name__
            if a != b:
                disagreements.append("%r -> free=%r method=%r" % (probe, a, b))
        self.assertFalse(
            disagreements,
            "the two copies of record_primary_hash have drifted:\n   "
            + "\n   ".join(disagreements[:8])
            + "\n\nThe field order is a record's identity precedence, so a disagreement files a "
              "record under a different hash rather than raising.")

    def test_they_are_still_two_objects(self):
        """If they ever become one, this file is the wrong shape and should say so rather than
        passing over a question that no longer exists."""
        free = matrixark_mcp_direct_cache.record_primary_hash
        method = matrixark_temporal_direct_read._TemporalDirectReadMixin._record_primary_hash
        self.assertIsNot(
            free, method,
            "the two are now one implementation -- replace this file with a guard that says so")

    def test_the_caches_the_dead_module_holds_are_not_the_live_ones(self):
        """The fact that makes adopting the sibling prune a bug, pinned so it cannot be forgotten.

        If these ever become the same object, adopting `prune_retrieval_candidate_cache` becomes
        safe -- and this assertion is what should be read before anyone tries."""
        for name in ("_DIRECT_RETRIEVAL_CANDIDATE_CACHE",
                     "_DIRECT_PLACEMENT_CANDIDATE_TABLE_CACHE"):
            dead = getattr(matrixark_mcp_direct_cache, name, None)
            live = getattr(matrixark_temporal_direct_read, name, None)
            if dead is None or live is None:
                continue
            self.assertIsNot(
                dead, live,
                "%s is now shared between the two modules. That removes the reason "
                "`prune_retrieval_candidate_cache` could not be adopted -- re-read it." % name)


if __name__ == "__main__":
    unittest.main()
