# SPDX-License-Identifier: Apache-2.0
import asyncio
import unittest
from unittest.mock import patch

try:
    import temporalstore_sdk
except ImportError:
    temporalstore_sdk = None


@unittest.skipUnless(temporalstore_sdk is not None, "the TemporalStore Python SDK is required")
class TableFeastClientTest(unittest.TestCase):
    def setUp(self):
        from feast_temporalstore_command.table_client import TableFeastClient

        self.patch = patch("feast_temporalstore_command.table_client.TableClient")
        constructor = self.patch.start()
        self.addCleanup(self.patch.stop)
        self.table = constructor.return_value
        self.client = TableFeastClient(
            "example.test:17201", "feast", "online", "legacy", 9999, 40, 10, 1,
            4 * 1024 * 1024, 16, 2,
        )
        self.addCleanup(self.client.close)
        constructor.assert_called_once()
        self.assertEqual(constructor.call_args.args[1:3], ("feast", "online"))

    def test_distributed_table_requires_grpc_metadata(self):
        from feast_temporalstore_command.table_client import TableFeastClient

        with self.assertRaisesRegex(ValueError, "grpc:// or grpcs://"):
            TableFeastClient(
                "http://example.test", "feast", "online", "legacy", 9999, 40, 10, 1,
                4 * 1024 * 1024, 16, 2,
            )

    def test_sync_and_async_reads_preserve_missing_and_empty_binary_fields(self):
        pb = temporalstore_sdk.commands

        def serve(batch):
            command = batch[0].hash_multi_get
            self.assertEqual(list(command.fields), ["empty", "missing"])
            self.assertIn(command.key, ("key-a", "key-b"))
            return [pb.CommandResponse(
                status=pb.Status(ok=True), values=[b"", b""], values_present=bytes((1,)),
            )]

        self.table.batch_execute.side_effect = serve
        keys, fields = ["key-a", "key-b"], ["empty", "missing"]
        self.assertEqual(self.client.batch_hash_multi_get(keys, fields), [[b"", None]] * 2)
        self.assertEqual(asyncio.run(self.client.batch_hash_multi_get_async(keys, fields)),
                         [[b"", None]] * 2)

    def test_conditional_writes_preserve_ttl_and_count_skips(self):
        pb = temporalstore_sdk.commands
        self.table.batch_execute.return_value = [
            pb.CommandResponse(status=pb.Status(ok=True), integer=1),
            pb.CommandResponse(status=pb.Status(ok=True), integer=0),
        ]
        result = self.client.batch_hash_timestamp_set(
            ["value"], "_ts", [("first", 5, 2, [b""]), ("second", 3, 4, [b"abc"])],
            ttl_ms=30000,
        )
        self.assertEqual(result, (1, 1))
        first, second = [command.hash_multi_set_if_timestamp_older
                         for command in self.table.batch_execute.call_args.args[0]]
        self.assertEqual((first.key, first.timestamp_seconds, first.timestamp_nanos, first.ttl_ms),
                         ("first", 5, 2, 30000))
        self.assertEqual((first.entries[0].value, second.entries[0].value), (b"", b"abc"))

    def test_aggregate_preserves_negative_values_and_response_kind(self):
        pb = temporalstore_sdk.commands
        self.table.batch_execute.return_value = [
            pb.CommandResponse(status=pb.Status(ok=True), integer=-4)
        ]
        self.assertEqual(
            self.client.execute({
                "kind": "feature_proto_agg_query", "key": "series",
                "start_ms": 1000, "end_ms": 2000, "aggregator": "sum", "count": None,
            }),
            {"kind": "aggregate", "value": -4},
        )
        command = self.table.batch_execute.call_args.args[0][0].feature_proto_agg_query
        self.assertEqual(command.key, "series")
        self.assertFalse(command.HasField("count"))


@unittest.skipUnless(temporalstore_sdk is not None, "the TemporalStore Python SDK is required")
class TableFeastWireTest(unittest.TestCase):
    def test_routes_projected_reads_and_conditional_writes_to_the_elected_shards(self):
        from concurrent.futures import ThreadPoolExecutor
        from contextlib import ExitStack

        import grpc
        from temporalstore.v1.temporalstore_pb2_grpc import (
            MetadataServiceServicer, TemporalStoreServiceServicer,
            add_MetadataServiceServicer_to_server, add_TemporalStoreServiceServicer_to_server,
        )

        from feast_temporalstore_command.table_client import TableFeastClient

        pb = temporalstore_sdk.commands
        metadata_requests = []
        shard_requests = {}
        keys = ("123456789", "model/0/tp/1/rank/0/chunk/0000000000000000")
        read_fields = ("value", "empty", "missing")

        class DataShard(TemporalStoreServiceServicer):
            def __init__(self, shard_id, generation, key, value):
                self.shard_id, self.generation, self.key, self.value = shard_id, generation, key, value

            def BatchExecuteChecked(self, request, context):
                self_case.assertEqual((request.shard_id, request.load_version),
                                      (self.shard_id, self.generation))
                shard_requests.setdefault(self.shard_id, []).append([
                    command.WhichOneof("kind") for command in request.commands
                ])
                responses = []
                for command in request.commands:
                    if command.HasField("hash_multi_get"):
                        read = command.hash_multi_get
                        self_case.assertEqual((read.key, tuple(read.fields)), (self.key, read_fields))
                        responses.append(pb.CommandResponse(
                            status=pb.Status(ok=True), values=[self.value, b"", b""],
                            values_present=b"\x03",
                        ))
                    else:
                        self_case.assertTrue(command.HasField("hash_multi_set_if_timestamp_older"))
                        write = command.hash_multi_set_if_timestamp_older
                        self_case.assertEqual((write.key, write.ttl_ms), (self.key, 30000))
                        self_case.assertEqual(write.entries[0].value, self.value)
                        responses.append(pb.CommandResponse(
                            status=pb.Status(ok=True), integer=int(self.shard_id == 74),
                        ))
                return pb.BatchExecuteResponse(status=pb.Status(ok=True), responses=responses)

        class Metadata(MetadataServiceServicer):
            def GetTableTopology(self, request, context):
                metadata_requests.append((request.namespace_name, request.table_name))
                topology = pb.MetadataTableTopologyResponse(
                    status=pb.Status(ok=True), table_id=7, topology_version=42,
                    first_shard_id=73, shard_count=2,
                )
                for shard_id, generation, target in routes:
                    topology.shards.add(shard_id=shard_id, uses_raft_route=True,
                                        primary_grpc=target, load_version=generation)
                return topology

        self_case = self
        with ExitStack() as stack:
            def start(servicer, register):
                workers = stack.enter_context(ThreadPoolExecutor(max_workers=2))
                server = grpc.server(workers)
                register(servicer, server)
                port = server.add_insecure_port("127.0.0.1:0")
                server.start()
                stack.callback(lambda: server.stop(0).wait(timeout=3))
                return f"127.0.0.1:{port}"

            values = (b"J\x00\xff", b"I\x00\xff")
            routes = [
                (shard, version, start(DataShard(shard, version, key, value),
                                       add_TemporalStoreServiceServicer_to_server))
                for shard, version, key, value in zip((74, 73), (11, 7), keys, values)
            ]
            metadata = start(Metadata(), add_MetadataServiceServicer_to_server)
            client = TableFeastClient(
                f"grpc://{metadata}", "feast", "online", "legacy", 9999, 40, 3, 1,
                4 * 1024 * 1024, 16, 2,
            )
            stack.callback(client.close)

            projection = [[value, b"", None] for value in values]
            self.assertEqual(client.batch_hash_multi_get(keys, read_fields), projection)
            self.assertEqual(asyncio.run(client.batch_hash_multi_get_async(keys, read_fields)),
                             projection)
            records = [(key, 5, 2, [value]) for key, value in zip(keys, values)]
            self.assertEqual(client.batch_hash_timestamp_set(
                ["value"], "_ts", records, ttl_ms=30000,
            ), (1, 1))
            self.assertEqual(metadata_requests, [("feast", "online")])
            self.assertEqual(shard_requests, {
                shard: [["hash_multi_get"], ["hash_multi_get"],
                        ["hash_multi_set_if_timestamp_older"]]
                for shard in (73, 74)
            })
