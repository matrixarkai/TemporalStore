# SPDX-License-Identifier: Apache-2.0
import unittest

from temporalstore_sdk import MetadataError, Shard, TableTopology


class TablePlacementTest(unittest.TestCase):
    def test_shard_assignment_matches_the_rust_sdk(self):
        topology = TableTopology(
            "kvcache", "model", 1, 73, 17,
            tuple(Shard(shard_id, None, (), 0) for shard_id in range(73, 90)),
        )
        for key, shard_id in (("123456789", 87), ("", 73), ("alpha", 84), ("é", 82),
                              ("mixed/🦀/key", 78), ("model@rank#abc", 76)):
            with self.subTest(key=key):
                self.assertEqual(topology.shard_for_key(key).shard_id, shard_id)

    def test_incomplete_topology_rejects_unknown_shard(self):
        topology = TableTopology("kvcache", "model", 1, 73, 2, ())
        with self.assertRaisesRegex(MetadataError, "missing shard"):
            topology.shard_for_key("alpha")
