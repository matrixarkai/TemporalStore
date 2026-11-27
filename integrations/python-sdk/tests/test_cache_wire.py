# SPDX-License-Identifier: Apache-2.0
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from unittest.mock import Mock

import grpc
from temporalstore.v1 import temporalstore_pb2 as pb
from temporalstore.v1.temporalstore_pb2_grpc import (
    TemporalStoreServiceServicer, add_TemporalStoreServiceServicer_to_server,
)
from temporalstore_sdk import Shard, TableClient, TableTopology
from temporalstore_sdk.cache_load_wire import LoadBatch, decode_load_batch
from temporalstore_sdk.cache_wire import CacheInsert, encode_cache_batch


def test_borrowed_insert_matches_generated_request_with_mixed_commands():
    for key in ("", "kvcache/模型"):
        for ttl in (None, 0, 42):
            for load_version in (None, 0, (1 << 60) + 7):
                value = memoryview(bytearray(range(256)))
                ordinary = pb.Command(string_get=pb.StringGet(key=key))
                conditional = pb.StringSetConditional(
                    key=key, value=bytes(value), condition=pb.STRING_SET_CONDITION_IF_NOT_EXISTS,
                )
                if ttl is not None:
                    conditional.ttl_ms = ttl
                expected_message = pb.BatchExecuteRequest(
                    shard_id=73, commands=[ordinary, pb.Command(string_set_conditional=conditional)],
                )
                if load_version is not None:
                    expected_message.load_version = load_version
                assert encode_cache_batch(
                    73, [ordinary, CacheInsert(key, value, ttl)], load_version,
                ) == expected_message.SerializeToString()


def test_borrowed_tensor_round_trips_through_checked_table_grpc():
    value = memoryview(bytearray(bytes(range(256)) * 4096))
    observed = []

    class Receiver(TemporalStoreServiceServicer):
        def BatchExecuteChecked(self, request, context):
            observed.append(request)
            return pb.BatchExecuteResponse(
                status=pb.Status(ok=True),
                responses=[pb.CommandResponse(status=pb.Status(ok=True), count=1)],
            )

    with ExitStack() as stack:
        server = grpc.server(stack.enter_context(ThreadPoolExecutor(max_workers=1)),
                             options=[("grpc.max_receive_message_length", 40 * 1024 * 1024)])
        add_TemporalStoreServiceServicer_to_server(Receiver(), server)
        port = server.add_insecure_port("127.0.0.1:0")
        server.start()
        stack.callback(lambda: server.stop(0).wait(3))
        metadata = Mock()
        metadata.table_topology.return_value = TableTopology(
            "kvcache", "model", 1, 73, 1, (Shard(73, None, (), 7, f"127.0.0.1:{port}"),),
        )
        table = stack.enter_context(TableClient(metadata, "kvcache", "model", timeout_seconds=3))
        result, = table.batch_execute([CacheInsert("tensor", value, 5_000)])
        assert result.status.ok and result.count == 1

    request, = observed
    assert request.shard_id == 73 and request.load_version == 7
    command, = request.commands
    insert = command.string_set_conditional
    assert insert.key == "tensor" and insert.value == value and insert.ttl_ms == 5_000
    assert insert.condition == pb.STRING_SET_CONDITION_IF_NOT_EXISTS
    assert not insert.return_old and not insert.keep_ttl



def test_load_decoder_retains_tonic_typed_bytes_including_empty_and_missing():
    status = pb.Status(ok=True)
    encoded = pb.BatchExecuteResponse(
        status=status,
        responses=[
            pb.CommandResponse(status=status, result_kind=pb.COMMAND_RESULT_KIND_BYTES,
                               value=b"tensor", value_present=True),
            pb.CommandResponse(status=status, result_kind=pb.COMMAND_RESULT_KIND_BYTES,
                               value_present=True),
            pb.CommandResponse(status=status, result_kind=pb.COMMAND_RESULT_KIND_BYTES,
                               value_present=False),
        ],
    ).SerializeToString()
    decoded = decode_load_batch(encoded)
    assert isinstance(decoded, LoadBatch)
    assert decoded.status.ok and all(reply.status.ok for reply in decoded.responses)
    assert [reply.value for reply in decoded.responses] == [b"tensor", b"", b""]
    assert [reply.value_present for reply in decoded.responses] == [True, True, False]
    assert all(reply.value.obj is encoded for reply in decoded.responses)


def test_load_decoder_keeps_generated_non_byte_replies():
    encoded = pb.BatchExecuteResponse(
        responses=[pb.CommandResponse(result_kind=pb.COMMAND_RESULT_KIND_INTEGER, count=5)],
    ).SerializeToString()
    decoded = decode_load_batch(encoded)
    assert isinstance(decoded, pb.BatchExecuteResponse)
    assert decoded.responses[0].count == 5
