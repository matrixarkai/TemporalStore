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
            client.batch_execute.side_effect = lambda shard, batch, **_: commands.BatchExecuteResponse(
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

    def test_checked_batches_use_the_topology_load_version(self):
        metadata = Mock()
        metadata.table_topology.return_value = TableTopology(
            "kvcache", "model", 1, 73, 2,
            (Shard(73, None, (), 0, "primary-a"), Shard(74, None, (), 19, "primary-b")),
        )
        with patch("temporalstore_sdk.table.CommandClient") as channel:
            channel.return_value.batch_execute.return_value = commands.BatchExecuteResponse(
                responses=[commands.CommandResponse()],
            )
            with TableClient(metadata, "kvcache", "model") as table:
                table.batch_execute([get("123456789")])
                channel.assert_called_once_with("primary-b", timeout_seconds=10.0)
                self.assertEqual(channel.return_value.batch_execute.call_args.args[0], 74)
                self.assertEqual(channel.return_value.batch_execute.call_args.kwargs,
                                 {"load_version": 19})

    def test_stale_fence_expires_route_without_retrying_the_failed_write(self):
        metadata = Mock()
        metadata.table_topology.side_effect = [
            TableTopology("kvcache", "model", 1, 73, 1, (Shard(73, None, (), 7, "primary-a"),)),
            TableTopology("kvcache", "model", 2, 73, 1, (Shard(73, None, (), 8, "primary-a"),)),
        ]
        with patch("temporalstore_sdk.table.CommandClient") as channel:
            channel.return_value.batch_execute.side_effect = [
                commands.BatchExecuteResponse(
                    status=commands.Status(code="load_version_mismatch"),
                ),
                commands.BatchExecuteResponse(
                    responses=[commands.CommandResponse(count=1)],
                ),
            ]
            with TableClient(metadata, "kvcache", "model") as table:
                with self.assertRaises(MetadataError) as stale:
                    table.batch_execute([get("a")])
                self.assertEqual(stale.exception.code, "load_version_mismatch")
                self.assertEqual(channel.return_value.batch_execute.call_count, 1)
                self.assertEqual(table.batch_execute([get("a")])[0].count, 1)
                self.assertEqual(metadata.table_topology.call_count, 2)
                calls = channel.return_value.batch_execute.call_args_list
                self.assertEqual([call.kwargs["load_version"] for call in calls], [7, 8])
