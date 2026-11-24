# SPDX-License-Identifier: Apache-2.0
"""Table-scoped data routing through the metaserver."""

from math import isfinite
from threading import Lock
from time import monotonic
from typing import Iterable, Iterator

import grpc
from temporalstore.v1 import temporalstore_pb2 as pb

from .client import CommandClient
from .metadata import MetadataClient, MetadataError, Shard, TableTopology


class TableClient:
    """Route keyed protobuf commands to a table's advertised primary."""

    def __init__(
        self,
        metadata: MetadataClient,
        namespace: str,
        table: str,
        *,
        topology_ttl_seconds: float = 5.0,
        timeout_seconds: float = 10.0,
    ):
        if not namespace or not table:
            raise ValueError("namespace and table are required")
        if not isfinite(topology_ttl_seconds) or topology_ttl_seconds <= 0:
            raise ValueError("topology TTL must be finite and positive")
        self._metadata = metadata
        self._namespace = namespace
        self._table = table
        self._topology_ttl = topology_ttl_seconds
        self._timeout = timeout_seconds
        self._topology = None
        self._expires_at = 0.0
        self._channels: dict[str, CommandClient] = {}
        self._lock = Lock()
        self._closed = False

    def _current_topology(self) -> TableTopology:
        with self._lock:
            if self._closed:
                raise RuntimeError("table client is closed")
            if self._topology is None or monotonic() >= self._expires_at:
                self._topology = self._metadata.table_topology(self._namespace, self._table)
                self._expires_at = monotonic() + self._topology_ttl
            return self._topology

    @staticmethod
    def _shard_for_command(topology: TableTopology, command: pb.Command) -> Shard:
        kind = command.WhichOneof("kind")
        body = getattr(command, kind) if kind is not None else None
        key = getattr(body, "key", None)
        if not isinstance(key, str):
            raise ValueError("table routing requires a single-key command")
        return topology.shard_for_key(key)

    def _channel_for(self, shard: Shard) -> CommandClient:
        if not shard.primary_grpc:
            raise MetadataError("no_grpc_primary", f"shard {shard.shard_id} has no gRPC primary")
        with self._lock:
            if self._closed:
                raise RuntimeError("table client is closed")
            channel = self._channels.get(shard.primary_grpc)
            if channel is None:
                channel = CommandClient(shard.primary_grpc, timeout_seconds=self._timeout)
                self._channels[shard.primary_grpc] = channel
            return channel

    def _invalidate_route(self, topology: TableTopology) -> None:
        with self._lock:
            if self._topology is topology:
                self._expires_at = 0.0

    def execute(self, command: pb.Command) -> pb.ExecuteResponse:
        topology = self._current_topology()
        shard = self._shard_for_command(topology, command)
        channel = self._channel_for(shard)
        try:
            response = channel.execute(shard.shard_id, command, load_version=shard.load_version or None)
        except grpc.RpcError as error:
            if error.code() == grpc.StatusCode.UNAVAILABLE:
                self._invalidate_route(topology)
            raise
        if response.status.code in {"load_version_mismatch", "raft_error"}:
            self._invalidate_route(topology)
        return response

    def batch_execute(self, commands: Iterable[pb.Command]) -> list[pb.CommandResponse]:
        indexed = sorted(self.iter_batch_responses(commands), key=lambda item: item[0])
        return [result for _, result in indexed]

    def iter_batch_responses(
        self, commands: Iterable[pb.Command],
    ) -> Iterator[tuple[int, pb.CommandResponse]]:
        """Yield confirmed results with their request positions as shards respond."""
        commands = list(commands)
        if not commands:
            return
        topology = self._current_topology()
        groups: dict[Shard, list[tuple[int, pb.Command]]] = {}
        for position, command in enumerate(commands):
            shard = self._shard_for_command(topology, command)
            groups.setdefault(shard, []).append((position, command))
        # Validate all routes before issuing any writes.
        channels = {shard: self._channel_for(shard) for shard in groups}
        for shard, group in groups.items():
            try:
                response = channels[shard].batch_execute(
                    shard.shard_id, (command for _, command in group),
                    load_version=shard.load_version or None,
                )
            except grpc.RpcError as error:
                if error.code() == grpc.StatusCode.UNAVAILABLE:
                    self._invalidate_route(topology)
                raise
            if response.status.code in {"load_version_mismatch", "raft_error"}:
                self._invalidate_route(topology)
            if len(response.responses) != len(group):
                if not response.status.ok:
                    raise MetadataError(response.status.code, response.status.message)
                raise MetadataError("invalid_batch", "datanode returned the wrong response count")
            for (position, _), result in zip(group, response.responses):
                yield position, result

    def close(self) -> None:
        with self._lock:
            self._closed = True
            channels, self._channels = self._channels, {}
        for channel in channels.values():
            channel.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()
