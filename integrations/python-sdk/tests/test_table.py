# SPDX-License-Identifier: Apache-2.0
import unittest
from unittest.mock import Mock, patch

import grpc

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

    def test_transport_unavailable_refreshes_next_request_without_resending_writes(self):
        class Unavailable(grpc.RpcError):
            def code(self):
                return grpc.StatusCode.UNAVAILABLE

        for rpc in ("execute", "batch_execute"):
            with self.subTest(rpc=rpc):
                metadata = Mock()
                metadata.table_topology.side_effect = [
                    TableTopology("kvcache", "model", 1, 73, 1,
                                  (Shard(73, None, (), 7, "primary-a"),)),
                    TableTopology("kvcache", "model", 2, 73, 1,
                                  (Shard(73, None, (), 8, "primary-b"),)),
                ]
                first, second = Mock(), Mock()
                getattr(first, rpc).side_effect = Unavailable
                getattr(second, rpc).return_value = (
                    commands.ExecuteResponse(status=commands.Status(ok=True))
                    if rpc == "execute" else commands.BatchExecuteResponse(
                        responses=[commands.CommandResponse(count=1)],
                    )
                )
                write = commands.Command(string_set=commands.StringSet(key="a", value=b"value"))
                request = write if rpc == "execute" else [write]
                with patch("temporalstore_sdk.table.CommandClient", side_effect=[first, second]):
                    with TableClient(metadata, "kvcache", "model") as table:
                        call = getattr(table, rpc)
                        with self.assertRaises(Unavailable):
                            call(request)
                        getattr(first, rpc).assert_called_once()
                        getattr(second, rpc).assert_not_called()
                        self.assertEqual(metadata.table_topology.call_count, 1)
                        call(request)
                        self.assertEqual(metadata.table_topology.call_count, 2)
                        self.assertEqual(getattr(second, rpc).call_args.kwargs["load_version"], 8)

    def test_missing_elected_leader_refreshes_before_normal_topology_ttl(self):
        metadata = Mock()
        metadata.table_topology.side_effect = [
            TableTopology("kvcache", "model", 1, 73, 1,
                          (Shard(73, None, (), 7, uses_raft_route=True),)),
            TableTopology("kvcache", "model", 2, 73, 1,
                          (Shard(73, "leader-http", (), 7, "leader-grpc", uses_raft_route=True),)),
        ]
        with patch("temporalstore_sdk.table.monotonic") as clock, patch(
            "temporalstore_sdk.table.CommandClient"
        ) as client:
            client.return_value.execute.return_value = commands.ExecuteResponse(
                status=commands.Status(ok=True),
            )
            with TableClient(metadata, "kvcache", "model", topology_ttl_seconds=5) as table:
                for now in (0, 0.1):
                    clock.return_value = now
                    with self.assertRaisesRegex(MetadataError, "no_grpc_primary"):
                        table.execute(get("cached-key"))
                self.assertEqual(metadata.table_topology.call_count, 1)
                clock.return_value = 0.3
                self.assertTrue(table.execute(get("cached-key")).status.ok)
                clock.return_value = 0.6
                self.assertTrue(table.execute(get("cached-key")).status.ok)
                self.assertEqual(metadata.table_topology.call_count, 2)
            client.assert_called_once_with("leader-grpc", timeout_seconds=10)
