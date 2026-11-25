# SPDX-License-Identifier: Apache-2.0
"""Blocking gRPC transport for one TemporalStore endpoint."""

from typing import Iterable, Optional

import grpc
from temporalstore.v1 import temporalstore_pb2 as pb
from temporalstore.v1.temporalstore_pb2_grpc import TemporalStoreServiceStub


def write_outcome_is_unknown(error: grpc.RpcError) -> bool:
    """Identify transport failures that may have interrupted an accepted write."""
    code = error.code()
    if code in (grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED):
        return True
    # The server's timeout layer can report its deadline as CANCELLED.
    return code == grpc.StatusCode.CANCELLED and error.details() in {
        "Timeout expired", "Timeout%20expired",
    }


def command_channel(target: str, credentials: Optional[grpc.ChannelCredentials]) -> grpc.Channel:
    limit = 40 * 1024 * 1024
    options = (
        ("grpc.max_send_message_length", limit),
        ("grpc.max_receive_message_length", limit),
    )
    if credentials is None:
        return grpc.insecure_channel(target, options=options)
    return grpc.secure_channel(target, credentials, options=options)


class CommandClient:
    """Send typed commands over one reusable, thread-safe gRPC channel."""

    def __init__(
        self,
        target: str,
        *,
        timeout_seconds: float = 10.0,
        credentials: Optional[grpc.ChannelCredentials] = None,
    ):
        self._channel = command_channel(target, credentials)
        self._service = TemporalStoreServiceStub(self._channel)
        self._timeout = timeout_seconds

    def execute(
        self, shard_id: int, command: pb.Command, *, load_version: Optional[int] = None,
        timeout_seconds: Optional[float] = None,
    ) -> pb.ExecuteResponse:
        request = pb.ExecuteRequest(shard_id=shard_id, command=command)
        timeout = self._timeout if timeout_seconds is None else timeout_seconds
        if load_version is not None:
            request.load_version = load_version
            return self._service.ExecuteChecked(request, timeout=timeout)
        return self._service.Execute(request, timeout=timeout)

    def _batch_call(self, shard_id, commands, load_version):
        request = pb.BatchExecuteRequest(shard_id=shard_id, commands=commands)
        if load_version is not None:
            request.load_version = load_version
            return self._service.BatchExecuteChecked, request
        return self._service.BatchExecute, request

    def batch_execute(
        self, shard_id: int, commands: Iterable[pb.Command], *,
        load_version: Optional[int] = None, timeout_seconds: Optional[float] = None,
    ) -> pb.BatchExecuteResponse:
        call, request = self._batch_call(shard_id, commands, load_version)
        timeout = self._timeout if timeout_seconds is None else timeout_seconds
        return call(request, timeout=timeout)

    def _start_batch(self, shard_id, commands, *, load_version, timeout_seconds=None):
        call, request = self._batch_call(shard_id, commands, load_version)
        timeout = self._timeout if timeout_seconds is None else timeout_seconds
        return call.future(request, timeout=timeout)

    def open_table(self, namespace: str, table: str, location: str = "") -> pb.OpenTableResponse:
        request = pb.OpenTableRequest(
            namespace_name=namespace, table_name=table, local_location=location,
        )
        return self._service.OpenTable(request, timeout=self._timeout)

    def sync_topology(self, tables: Iterable[str]) -> pb.SyncTopologyResponse:
        request = pb.SyncTopologyRequest(table_keys=tables)
        return self._service.SyncTopology(request, timeout=self._timeout)

    def preflight(self) -> pb.ClientPreflightResponse:
        return self._service.GetClientPreflight(
            pb.ClientPreflightRequest(), timeout=self._timeout,
        )

    def close(self) -> None:
        self._channel.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()
