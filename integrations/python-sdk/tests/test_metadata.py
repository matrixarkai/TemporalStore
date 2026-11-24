# SPDX-License-Identifier: Apache-2.0
import json
from pathlib import Path
import unittest

from temporalstore_sdk import MetadataError, Shard, TableTopology
from temporalstore_sdk.metadata import _crc64


class TablePlacementTest(unittest.TestCase):
    def test_shard_assignment_matches_the_rust_sdk(self):
        fixture_path = Path(__file__).resolve().parents[3] / (
            "crates/temporalstore-rust/tests/fixtures/python_sdk_shard_routing.json"
        )
        fixture = json.loads(fixture_path.read_text("utf-8"))
        vector, crc = fixture["crc_vector"]
        self.assertEqual(f"{_crc64(vector):016x}", crc)
        first = fixture["first_shard_id"]
        for index, count in enumerate(fixture["shard_counts"]):
            topology = TableTopology(
                "kvcache", "model", 1, first, count,
                tuple(Shard(shard_id, None, (), 0) for shard_id in range(first, first + count)),
            )
            for key, *shard_ids in fixture["keys"]:
                with self.subTest(key=key, count=count):
                    self.assertEqual(topology.shard_for_key(key).shard_id, shard_ids[index])

    def test_incomplete_topology_rejects_unknown_shard(self):
        topology = TableTopology("kvcache", "model", 1, 73, 2, ())
        with self.assertRaisesRegex(MetadataError, "missing shard"):
            topology.shard_for_key("alpha")
