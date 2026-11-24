# SPDX-License-Identifier: Apache-2.0
"""Blocking gRPC transport for one TemporalStore endpoint."""

from typing import Iterable, Optional

import grpc
from temporalstore.v1 import temporalstore_pb2 as pb
from temporalstore.v1.temporalstore_pb2_grpc import TemporalStoreServiceStub


class CommandClient:
    """Send typed commands over one reusable, thread-safe gRPC channel."""

    def __init__(
        self,
        target: str,
        *,
        timeout_seconds: float = 10.0,
        credentials: Optional[grpc.ChannelCredentials] = None,
    ):
        message_limit = 40 * 1024 * 1024
        options = (
            ("grpc.max_send_message_length", message_limit),
            ("grpc.max_receive_message_length", message_limit),
        )
        self._channel = (
            grpc.insecure_channel(target, options=options)
            if credentials is None
            else grpc.secure_channel(target, credentials, options=options)
        )
        self._service = TemporalStoreServiceStub(self._channel)
        self._timeout = timeout_seconds

    def execute(
        self, shard_id: int, command: pb.Command, *, load_version: Optional[int] = None,
    ) -> pb.ExecuteResponse:
        request = pb.ExecuteRequest(shard_id=shard_id, command=command)
        if load_version is not None:
            request.load_version = load_version
            return self._service.ExecuteChecked(request, timeout=self._timeout)
        return self._service.Execute(request, timeout=self._timeout)

    def batch_execute(
        self, shard_id: int, commands: Iterable[pb.Command], *, load_version: Optional[int] = None,
    ) -> pb.BatchExecuteResponse:
        request = pb.BatchExecuteRequest(shard_id=shard_id, commands=commands)
        if load_version is not None:
            request.load_version = load_version
            return self._service.BatchExecuteChecked(request, timeout=self._timeout)
        return self._service.BatchExecute(request, timeout=self._timeout)

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
