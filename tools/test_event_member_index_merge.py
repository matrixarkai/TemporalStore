"""The membership index is now merged incrementally on append instead of being dropped and
rebuilt from the whole store. Guard the property that makes that sound: a merge must produce
exactly what a full rebuild would, and a removal must NOT take the merge path.
"""
import unittest

try:  # package path
    from tools.matrixark_mcp_local_adapter import (
        MEMORY_TOMBSTONE_RECORD_TYPE,
        build_event_member_index,
    )
except ImportError:  # top-level path (direct tools/ execution)
    from matrixark_mcp_local_adapter import (
        MEMORY_TOMBSTONE_RECORD_TYPE,
        build_event_member_index,
    )


def event(event_id):
    return {"record_type": "context_event", "event_id_hash": event_id}


def entity(entity_hash, source_event_id):
    return {
        "record_type": "context_entity",
        "entity_hash": entity_hash,
        "source_event_ids": [source_event_id],
    }


class MergeEqualsRebuild(unittest.TestCase):
    def merged(self, *batches):
        index = {}
        for batch in batches:
            for key, members in build_event_member_index(list(batch)).items():
                index.setdefault(key, set()).update(members)
        return index

    def test_merging_batches_equals_building_them_all_at_once(self):
        first = [event(100), entity("900", 100)]
        second = [event(200), entity("901", 200)]
        third = [entity("902", 100)]  # a later derivative of an EARLIER event
        rebuilt = build_event_member_index(first + second + third)
        self.assertEqual(self.merged(first, second, third), rebuilt)
        # Not vacuous: the index must actually carry the entries, or two empty dicts
        # would compare equal and prove nothing.
        self.assertIn("100", rebuilt)
        self.assertIn("902", rebuilt["100"])

    def test_a_derivative_arriving_after_its_source_still_lands(self):
        # The ordering a drop-and-rebuild handled for free and a merge must handle
        # explicitly: the derivative is appended in a LATER batch than its source event.
        index = self.merged([event(100)], [entity("903", 100)])
        self.assertEqual(index["100"], {"100", "903"})

    def test_membership_is_additive_so_a_union_cannot_lose_a_member(self):
        # The premise of the optimisation: re-applying a batch is idempotent, and batch
        # order does not matter.
        a = [event(100), entity("900", 100)]
        b = [entity("901", 100)]
        self.assertEqual(self.merged(a, b), self.merged(b, a))
        self.assertEqual(self.merged(a, b), self.merged(a, b, b))

    def test_a_tombstone_contributes_nothing_to_the_index(self):
        # Why a tombstone must invalidate rather than merge: it builds to nothing here, so
        # folding it in as a union would leave the removed members in place.
        tombstone = {
            "record_type": MEMORY_TOMBSTONE_RECORD_TYPE,
            "tombstone_kind": "delete",
            "target_memory_id": "100",
        }
        self.assertEqual(build_event_member_index([tombstone]), {})
        # And the members it removes ARE present beforehand, so the union really would
        # have kept something stale.
        self.assertIn("100", build_event_member_index([event(100), entity("900", 100)]))


if __name__ == "__main__":
    unittest.main()
