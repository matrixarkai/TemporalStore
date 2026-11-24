# SPDX-License-Identifier: Apache-2.0
import unittest
from unittest.mock import Mock, patch

from temporalstore_sdk import MetadataError, Shard, TableClient, TableTopology, commands


def get(key):
    return commands.Command(string_get=commands.StringGet(key=key))


class TableClientTest(unittest.TestCase):
    def test_batches_by_advertised_primary_and_restores_request_order(self):
        metadata = Mock()
        metadata.table_topology.return_value = TableTopology(
            "kvcache", "model", 1, 73, 2,
            (Shard(73, None, (), 0, "primary-a"), Shard(74, None, (), 0, "primary-b")),
        )
        channels = {}

        def channel(target, **_):
            client = channels.setdefault(target, Mock())
            client.batch_execute.side_effect = lambda shard, batch: commands.BatchExecuteResponse(
                responses=[commands.CommandResponse(
                    value=f"{shard}/{request.string_get.key}".encode(),
                ) for request in batch],
            )
            return client

        keys = ["alpha", "", "123456789", "é"]
        with patch("temporalstore_sdk.table.CommandClient", side_effect=channel) as create:
            with TableClient(metadata, "kvcache", "model") as table:
                expected = [b"73/alpha", b"73/", b"74/123456789", "74/é".encode()]
                self.assertEqual([result.value for result in table.batch_execute(map(get, keys))],
                                 expected)
                table.batch_execute(map(get, keys))
                self.assertEqual(create.call_count, 2)
                metadata.table_topology.assert_called_once_with("kvcache", "model")
        for client in channels.values():
            client.close.assert_called_once_with()

    def test_fenced_shard_is_rejected_before_any_commands_are_sent(self):
        metadata = Mock()
        metadata.table_topology.return_value = TableTopology(
            "kvcache", "model", 1, 73, 2,
            (Shard(73, None, (), 0, "primary-a"), Shard(74, None, (), 19, "primary-b")),
        )
        with patch("temporalstore_sdk.table.CommandClient") as channel:
            with TableClient(metadata, "kvcache", "model") as table:
                with self.assertRaises(MetadataError) as raised:
                    table.batch_execute([get(""), get("123456789")])
                self.assertEqual(raised.exception.code, "fencing_required")
                channel.return_value.batch_execute.assert_not_called()
