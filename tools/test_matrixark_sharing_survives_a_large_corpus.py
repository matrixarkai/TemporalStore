"""A field that repeats must keep being shared however many distinct values it holds.

The shared table used to stop at 4,096 entries -- a ceiling picked when the busiest field on the
measured corpus held 11 distinct values, so it could not plausibly bind. At 100,105 records it
binds: that corpus holds 29,657 distinct vectors, so every value past the 4,096th was handed back
unshared, silently, costing 66.8 MB of duplicates. What replaces the ceiling is a per-field
hit-rate test, so the guard falls on fields that do not repeat instead of on whichever field
happened to fill the table first.

The corpus modelled here is the one that matters: a chunk body is stored once as a skill_section
and once as a resource_chunk, so duplicates arrive in adjacent pairs.
"""
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import matrixark_mcp_local_adapter as adapter_module


DISTINCT = 6000         # comfortably past the old 4,096 ceiling
VECTOR_WIDTH = 8


def _paired_rows(distinct, copies=2, field="vector"):
    """``distinct`` values, each repeated ``copies`` times, duplicates adjacent."""
    return [
        {"record_type": "skill_section",
         "node_id": "n-%d" % i,
         field: [float((i // copies) * VECTOR_WIDTH + d) for d in range(VECTOR_WIDTH)]}
        for i in range(distinct * copies)
    ]


def _all_distinct_rows(count, field="unique_ids"):
    return [
        {"record_type": "skill_section",
         "node_id": "n-%d" % i,
         field: [float(i * VECTOR_WIDTH + d) for d in range(VECTOR_WIDTH)]}
        for i in range(count)
    ]


class SharingSurvivesALargeCorpus(unittest.TestCase):
    #: Named rather than referenced directly so the fixture also runs against a build that has not
    #: got them yet -- the ceiling assertion should then fail on its own claim, which is the only
    #: way to know it discriminates, instead of erroring in setUp on a missing attribute.
    _TABLES = ("_SHARED_VALUE_TABLE", "_SHARED_CONTAINERS_ABANDONED",
               "_SHARED_CONTAINERS_EARNED", "_SHARED_CONTAINER_STATS")

    def setUp(self):
        # These tables are process-wide by design, so a test that reads them has to start clean --
        # and has to put back what it found, or it decides the behaviour of every test discovery
        # happens to run after it.
        self._saved = {}
        for name in self._TABLES:
            table = getattr(adapter_module, name, None)
            if table is None:
                continue
            self._saved[name] = table.copy()
            table.clear()

    def tearDown(self):
        for name, saved in self._saved.items():
            table = getattr(adapter_module, name)
            table.clear()
            table.update(saved)

    def test_a_repeating_field_is_shared_past_the_old_ceiling(self):
        rows = _paired_rows(DISTINCT)
        shared = adapter_module.share_repeated_values(rows, adapter_module._SHARED_VALUE_TABLE)

        objects = {id(record["vector"]) for record in shared}
        self.assertEqual(
            len(objects), DISTINCT,
            "%d rows over %d distinct vectors held %d objects; sharing stopped part way through "
            "the corpus" % (len(shared), DISTINCT, len(objects)),
        )

    def test_the_values_are_unchanged_by_sharing(self):
        """Non-vacuity, and the property that actually matters: sharing must not alter content."""
        rows = _paired_rows(DISTINCT)
        originals = [list(record["vector"]) for record in rows]
        shared = adapter_module.share_repeated_values(rows, adapter_module._SHARED_VALUE_TABLE)
        self.assertEqual(len(shared), len(originals))
        self.assertGreater(len(originals), 0)
        for original, record in zip(originals, shared):
            self.assertEqual(list(record["vector"]), original)

    def test_a_field_that_never_repeats_is_dropped(self):
        """The guard has to land somewhere -- on the field that is not repeating, now."""
        rows = _all_distinct_rows(DISTINCT, field="unique_ids")
        adapter_module.share_repeated_values(rows, adapter_module._SHARED_VALUE_TABLE)

        self.assertIn(
            "unique_ids", getattr(adapter_module, "_SHARED_CONTAINERS_ABANDONED", {}),
            "a field whose values are all distinct kept earning table entries that nothing will "
            "ever look up",
        )
        self.assertLess(
            len(adapter_module._SHARED_VALUE_TABLE), DISTINCT,
            "the abandoned field should have stopped adding entries well before the end",
        )

    def test_an_abandoned_field_still_returns_its_values(self):
        """Giving up on sharing must never change what the record holds."""
        rows = _all_distinct_rows(DISTINCT, field="unique_ids")
        originals = [list(record["unique_ids"]) for record in rows]
        shared = adapter_module.share_repeated_values(rows, adapter_module._SHARED_VALUE_TABLE)
        for original, record in zip(originals, shared):
            self.assertEqual(list(record["unique_ids"]), original)

    def test_a_field_whose_repeats_arrive_late_is_judged_again(self):
        """The verdict is reached on the first 512 lookups, so it must not be permanent.

        Here every distinct value is seen once before any of them repeats -- the warmup sees a
        0.00 hit rate and gives up. Without re-arming, the field would stay abandoned for the life
        of the process and the whole second half would go unshared.
        """
        distinct = _all_distinct_rows(DISTINCT, field="vector")
        rows = distinct + [dict(record) for record in distinct]
        shared = adapter_module.share_repeated_values(rows, adapter_module._SHARED_VALUE_TABLE)

        objects = {id(record["vector"]) for record in shared}
        self.assertLess(
            len(objects), len(rows),
            "a field abandoned during the warmup was never judged again, so nothing in the "
            "repeating half was shared",
        )


if __name__ == "__main__":
    unittest.main()
