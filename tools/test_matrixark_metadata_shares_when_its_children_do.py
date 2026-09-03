"""A dict that holds a container must still be shared once that container is.

Sharing keyed a dict by its contents, so a dict holding a list could not be keyed at all and was
returned unshared however often it repeated. That covered every `metadata` value in the measured
corpus: each one held exactly one container -- `heading_path` -- and each distinct value appeared
exactly twice, once as a skill_section and once as a resource_chunk. 99,320 objects for 49,641
values, 13.4% of the read cache carried at double.

Once the child is shared it is canonical -- one object per distinct value -- so the child's
identity keys the parent exactly.
"""
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import matrixark_mcp_local_adapter as adapter_module


DISTINCT = 900


def _rows(distinct, copies=2):
    """Records whose `metadata` holds a list, each distinct value repeated `copies` times."""
    return [
        {"record_type": "skill_section",
         "node_id": "n-%d" % i,
         "metadata": {"title": "r-%05d" % (i // copies),
                      "heading_path": ["Runbook", "Section %d" % (i // copies)]}}
        for i in range(distinct * copies)
    ]


class MetadataSharesWhenItsChildrenDo(unittest.TestCase):
    _TABLES = ("_SHARED_VALUE_TABLE", "_SHARED_CONTAINERS_ABANDONED",
               "_SHARED_CONTAINERS_EARNED", "_SHARED_CONTAINER_STATS")

    def setUp(self):
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

    def test_the_nested_child_is_shared(self):
        """Non-vacuity: the parent can only be keyed by a child that is itself canonical."""
        shared = adapter_module.share_repeated_values(_rows(DISTINCT),
                                                      adapter_module._SHARED_VALUE_TABLE)
        children = {id(record["metadata"]["heading_path"]) for record in shared}
        self.assertEqual(
            len(children), DISTINCT,
            "the nested list itself is not being shared, so nothing above it could be",
        )

    def test_the_parent_dict_is_shared_too(self):
        shared = adapter_module.share_repeated_values(_rows(DISTINCT),
                                                      adapter_module._SHARED_VALUE_TABLE)
        parents = {id(record["metadata"]) for record in shared}
        self.assertEqual(
            len(parents), DISTINCT,
            "%d rows over %d distinct metadata values held %d objects; a dict holding a list is "
            "still being passed through unshared" % (len(shared), DISTINCT, len(parents)),
        )

    def test_the_content_is_unchanged(self):
        rows = _rows(DISTINCT)
        originals = [{"title": r["metadata"]["title"],
                      "heading_path": list(r["metadata"]["heading_path"])} for r in rows]
        shared = adapter_module.share_repeated_values(rows, adapter_module._SHARED_VALUE_TABLE)
        self.assertEqual(len(shared), len(originals))
        for original, record in zip(originals, shared):
            self.assertEqual(record["metadata"]["title"], original["title"])
            self.assertEqual(list(record["metadata"]["heading_path"]), original["heading_path"])

    def test_two_values_differing_only_in_the_child_stay_apart(self):
        """The identity key must distinguish, not just collapse."""
        rows = [
            {"record_type": "skill_section", "node_id": "a",
             "metadata": {"title": "same", "heading_path": ["Runbook", "One"]}},
            {"record_type": "skill_section", "node_id": "b",
             "metadata": {"title": "same", "heading_path": ["Runbook", "Two"]}},
        ]
        shared = adapter_module.share_repeated_values(rows, adapter_module._SHARED_VALUE_TABLE)
        self.assertNotEqual(
            id(shared[0]["metadata"]), id(shared[1]["metadata"]),
            "two metadata values differing only inside the nested list were given the same object",
        )
        self.assertEqual(list(shared[0]["metadata"]["heading_path"]), ["Runbook", "One"])
        self.assertEqual(list(shared[1]["metadata"]["heading_path"]), ["Runbook", "Two"])

    def test_a_dict_holding_an_unshareable_value_is_passed_through(self):
        """A value that is neither a scalar nor a shared container must not be keyed by identity."""
        rows = [
            {"record_type": "skill_section", "node_id": "n-%d" % i,
             "metadata": {"title": "same", "opaque": {"nested": {"deep": {"deeper": i}}}}}
            for i in range(4)
        ]
        originals = [r["metadata"]["opaque"]["nested"]["deep"]["deeper"] for r in rows]
        shared = adapter_module.share_repeated_values(rows, adapter_module._SHARED_VALUE_TABLE)
        for original, record in zip(originals, shared):
            self.assertEqual(record["metadata"]["opaque"]["nested"]["deep"]["deeper"], original)


if __name__ == "__main__":
    unittest.main()
