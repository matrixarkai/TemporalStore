# SPDX-License-Identifier: Apache-2.0
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace

import grpc
import pytest
from temporalstore.v1.temporalstore_pb2_grpc import ProxyServiceServicer, add_ProxyServiceServicer_to_server
from temporalstore_sdk import MetadataError, ProxyCommandClient, ProxyTableClient, commands as pb


class EchoProxy(ProxyServiceServicer):
    def ProxyExecuteStream(self, requests, context):
        for request in requests:
            assert request.namespace_name == "kvcache"
            if request.table_name == "denied":
                yield pb.ProxyStreamEvent(status=pb.Status(code="proxy_account_denied"))
                continue
            if request.HasField("execute"):
                yield pb.ProxyStreamEvent(execute=pb.ExecuteResponse(
                    status=pb.Status(ok=True),
                    response=pb.CommandResponse(value=request.execute.command.string_set.value),
                ))
                continue
            results = [
                pb.CommandResponse(status=pb.Status(ok=True), value=command.string_set.value)
                for command in request.batch_execute.commands
            ]
            results[0].status.CopyFrom(pb.Status(code="raft_write_not_started"))
            yield pb.ProxyStreamEvent(
                status=results[0].status, batch_execute=pb.BatchExecuteResponse(responses=results),
            )


def test_proxy_table_client_preserves_binary_and_partial_outcomes():
    metadata = SimpleNamespace(table_topology=lambda *_: SimpleNamespace(
        table_id=51, version=2, shards=(),
    ))
    with ThreadPoolExecutor(max_workers=2) as workers:
        server = grpc.server(workers)
        add_ProxyServiceServicer_to_server(EchoProxy(), server)
        port = server.add_insecure_port("127.0.0.1:0")
        server.start()
        proxy = ProxyCommandClient(f"127.0.0.1:{port}")
        try:
            with ProxyTableClient(metadata, proxy, "kvcache", "model") as table:
                first = pb.Command(string_set=pb.StringSet(key="a", value=b"\x00\xff"))
                second = pb.Command(string_set=pb.StringSet(key="b", value=b"\x80\x00"))
                assert table.table_identity() == (51, None)
                assert table.execute(first).response.value == b"\x00\xff"
                results = table.batch_execute([first, second], timeout_seconds=2)
                assert [result.value for result in results] == [b"\x00\xff", b"\x80\x00"]
                assert results[0].status.code == "raft_write_not_started"
                assert results[1].status.ok
            with ProxyTableClient(metadata, proxy, "kvcache", "denied") as denied:
                with pytest.raises(MetadataError, match="proxy_account_denied"):
                    denied.batch_execute([first])
        finally:
            proxy.close()
            server.stop(0).wait()
