# SPDX-License-Identifier: Apache-2.0
"""Table-scoped commands through a TemporalStore protobuf proxy."""

from typing import Iterable, Iterator

import grpc
from temporalstore.v1 import temporalstore_pb2 as pb
from temporalstore.v1.temporalstore_pb2_grpc import ProxyServiceStub

from .client import command_channel
from .metadata import MetadataClient, MetadataError
from .table import TableClient


class ProxyCommandClient:
    """Share one gRPC channel across tables and command streams."""

    def __init__(
        self, target: str, *, credentials: grpc.ChannelCredentials | None = None,
    ) -> None:
        self._channel = command_channel(target, credentials)
        self._service = ProxyServiceStub(self._channel)

    def stream(self, requests: Iterable[pb.ProxyStreamRequest], *, timeout_seconds: float):
        """Return the live reply stream. Cancel it when leaving before EOF."""
        return self._service.ProxyExecuteStream(iter(requests), timeout=timeout_seconds)

    def exchange(self, request: pb.ProxyStreamRequest, *, timeout_seconds: float) -> pb.ProxyStreamEvent:
        replies = self.stream((request,), timeout_seconds=timeout_seconds)
        try:
            try:
                return next(replies)
            except StopIteration as error:
                raise MetadataError("invalid_batch", "proxy closed the stream without a reply") from error
        finally:
            replies.cancel()

    def close(self) -> None:
        self._channel.close()


class ProxyTableClient(TableClient):
    """Use metadata for table identity and an existing proxy channel for commands."""

    def __init__(
        self, metadata: MetadataClient, proxy: ProxyCommandClient, namespace: str, table: str,
        *, timeout_seconds: float = 10.0,
    ) -> None:
        super().__init__(metadata, namespace, table, timeout_seconds=timeout_seconds)
        self._proxy = proxy

    def _exchange(self, timeout_seconds: float | None = None, **command) -> pb.ProxyStreamEvent:
        with self._lock:
            if self._closed:
                raise RuntimeError("table client is closed")
        frame = pb.ProxyStreamRequest(namespace_name=self._namespace, table_name=self._table, **command)
        return self._proxy.exchange(
            frame, timeout_seconds=self._timeout if timeout_seconds is None else timeout_seconds,
        )

    def execute(self, command: pb.Command) -> pb.ExecuteResponse:
        event = self._exchange(execute=pb.ExecuteRequest(command=command))
        if event.HasField("execute"):
            return event.execute
        if event.HasField("status") and not event.status.ok:
            return pb.ExecuteResponse(status=event.status)
        raise MetadataError("invalid_response", "proxy did not return an execute reply")

    def iter_batch_responses(
        self, commands: Iterable[pb.Command], *, timeout_seconds: float | None = None,
    ) -> Iterator[tuple[int, pb.CommandResponse]]:
        commands = list(commands)
        if not commands:
            return
        event = self._exchange(timeout_seconds, batch_execute=pb.BatchExecuteRequest(commands=commands))
        results = event.batch_execute.responses
        if len(results) != len(commands):
            if event.HasField("status") and not event.status.ok:
                raise MetadataError(event.status.code, event.status.message)
            raise MetadataError("invalid_batch", "proxy returned the wrong response count")
        yield from enumerate(results)
