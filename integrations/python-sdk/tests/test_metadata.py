# SPDX-License-Identifier: Apache-2.0
import json
from pathlib import Path
import unittest

from temporalstore_sdk import MetadataClient, MetadataError, Shard, TableTopology
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

    def test_raft_routing_prefers_the_elected_endpoint_and_its_generation(self):
        row = {
            "shard_id": 73, "primary": "assigned:1000", "replicas": ["assigned:1000"],
            "primary_endpoint": {"grpc_addr": "assigned:2000"}, "load_version": 4,
            "raft_route": {
                "endpoint": {"server_addr": "leader:1000", "grpc_addr": "leader:2000"},
                "load_version": 7,
            },
        }
        shard = MetadataClient._decode_shard(row)
        self.assertEqual((shard.primary, shard.primary_grpc, shard.load_version),
                         ("leader:1000", "leader:2000", 7))
        row["raft_route"]["endpoint"] = None
        shard = MetadataClient._decode_shard(row)
        self.assertIsNone(shard.primary_grpc)
        self.assertIsNone(shard.primary)
        del row["raft_route"]
        shard = MetadataClient._decode_shard(row)
        self.assertEqual((shard.primary, shard.primary_grpc, shard.load_version),
                         ("assigned:1000", "assigned:2000", 4))


def test_metadata_grpc_uses_versioned_topology_and_bearer_credentials():
    from concurrent.futures import ThreadPoolExecutor
    from contextlib import ExitStack

    import grpc
    from temporalstore_sdk import TableClient, commands as pb

    seen = []
    calls = []

    def topology(request, context):
        seen.append((request.namespace_name, request.table_name, request.old_topology_version))
        assert ("authorization", "Bearer test-token") in context.invocation_metadata()
        response = pb.MetadataTableTopologyResponse(
            status=pb.Status(ok=True), table_id=7, topology_version=42, first_shard_id=73, shard_count=1,
            unchanged=bool(request.old_topology_version),
        )
        if not response.unchanged:
            response.shards.add(
                shard_id=73, uses_raft_route=True, primary_addr="leader:17002",
                primary_grpc=data_addr, load_version=8,
            )
        return response

    def execute(request, _context):
        calls.append((request.shard_id, request.load_version, request.commands[0].string_get.key))
        return pb.BatchExecuteResponse(
            status=pb.Status(ok=True),
            responses=[pb.CommandResponse(status=pb.Status(ok=True), value=b"\x00\xff")],
        )

    command_handler = grpc.unary_unary_rpc_method_handler(
        execute, request_deserializer=pb.BatchExecuteRequest.FromString,
        response_serializer=pb.BatchExecuteResponse.SerializeToString,
    )
    handler = grpc.unary_unary_rpc_method_handler(
        topology, request_deserializer=pb.MetadataTableTopologyRequest.FromString,
        response_serializer=pb.MetadataTableTopologyResponse.SerializeToString,
    )
    with ExitStack() as stack:
        worker = stack.enter_context(ThreadPoolExecutor(max_workers=1))
        data = grpc.server(worker)
        data.add_generic_rpc_handlers((
            grpc.method_handlers_generic_handler(
                "temporalstore.v1.TemporalStoreService", {"BatchExecuteChecked": command_handler},
            ),
        ))
        data_addr = f"127.0.0.1:{data.add_insecure_port('127.0.0.1:0')}"
        data.start()
        stack.callback(lambda: data.stop(0).wait(timeout=3))
        server = grpc.server(worker)
        server.add_generic_rpc_handlers((
            grpc.method_handlers_generic_handler("temporalstore.v1.MetadataService", {"GetTableTopology": handler}),
        ))
        port = server.add_insecure_port("127.0.0.1:0")
        server.start()
        stack.callback(lambda: server.stop(0).wait(timeout=3))
        client = MetadataClient(f"127.0.0.1:{port}", credential_provider=lambda: "test-token")
        stack.callback(client.close)
        first = client.table_topology("kvcache", "model")
        assert (first.table_id, first.version) == (7, 42)
        shard = first.shard_for_key("chunk")
        assert (shard.shard_id, shard.primary_grpc, shard.load_version) == (73, data_addr, 8)
        assert shard.uses_raft_route
        assert client.table_topology("kvcache", "model", previous=first) is first
        assert seen == [("kvcache", "model", 0), ("kvcache", "model", 42)]
        with TableClient(client, "kvcache", "model") as table:
            [reply] = table.batch_execute([pb.Command(string_get=pb.StringGet(key="chunk"))])
            assert reply.value == b"\x00\xff"
        assert calls == [(73, 8, "chunk")]

def test_topology_uses_the_smaller_explicit_or_default_timeout():
    from unittest.mock import Mock

    from temporalstore_sdk import commands as pb

    from contextlib import closing

    with closing(MetadataClient("grpc://127.0.0.1:1", timeout_seconds=5)) as client:
        client._topology_rpc = Mock(return_value=pb.MetadataTableTopologyResponse(
            status=pb.Status(ok=True), first_shard_id=73, shard_count=1,
        ))
        client.table_topology("kvcache", "model", timeout_seconds=0.25)
        client.table_topology("kvcache", "model", timeout_seconds=30)
        assert [call.kwargs["timeout"] for call in client._topology_rpc.call_args_list] == [0.25, 5]
