# SPDX-License-Identifier: Apache-2.0
"""Table-scoped data routing through the metaserver."""

from contextlib import closing, ExitStack
from itertools import islice
from math import isfinite
from threading import Lock
from time import monotonic
from typing import Iterable, Iterator

import grpc
from temporalstore.v1 import temporalstore_pb2 as pb

from .client import CommandClient
from .metadata import MetadataClient, MetadataError, RaftPrimaryUnavailable, Shard, TableTopology


_ELECTION_TOPOLOGY_TTL_SECONDS = 0.25

# The KV and Feast read paths can overlap independent shards. Writes and other
# commands retain their existing dispatch order.
_PARALLEL_READ_KINDS = frozenset({
    "common_exists", "common_ttl", "feature_agg_query", "feature_proto_agg_query",
    "hash_get", "hash_get_all", "hash_multi_get", "string_get", "string_get_range",
    "string_len",
})
_MAX_INFLIGHT_READ_SHARDS = 8




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
        parallel_reads: bool = False,
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
        self._parallel_reads = parallel_reads
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
                ttl = self._topology_ttl
                if any(shard.uses_raft_route and not shard.primary_grpc for shard in self._topology.shards):
                    ttl = min(ttl, _ELECTION_TOPOLOGY_TTL_SECONDS)
                self._expires_at = monotonic() + ttl
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
            if shard.uses_raft_route and shard.primary is None:
                raise RaftPrimaryUnavailable(shard.shard_id)
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

    def batch_execute(
        self, commands: Iterable[pb.Command], *, timeout_seconds: float | None = None,
    ) -> list[pb.CommandResponse]:
        indexed = sorted(
            self.iter_batch_responses(commands, timeout_seconds=timeout_seconds),
            key=lambda item: item[0],
        )
        return [result for _, result in indexed]

    def iter_batch_responses(
        self, commands: Iterable[pb.Command], *, timeout_seconds: float | None = None,
    ) -> Iterator[tuple[int, pb.CommandResponse]]:
        """Yield confirmed shard results with their original request positions."""
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
        parallel = self._parallel_reads and len(groups) > 1 and all(
            command.WhichOneof("kind") in _PARALLEL_READ_KINDS for command in commands
        )
        try:
            responses = self._batch_responses(groups, channels, parallel, timeout_seconds)
            with closing(responses) as batches:
                for group, response in batches:
                    if response.status.code in {"load_version_mismatch", "raft_error"}:
                        self._invalidate_route(topology)
                    if len(response.responses) != len(group):
                        if not response.status.ok:
                            raise MetadataError(response.status.code, response.status.message)
                        raise MetadataError("invalid_batch", "datanode returned the wrong response count")
                    for (position, _), result in zip(group, response.responses):
                        yield position, result
        except grpc.RpcError as error:
            if error.code() == grpc.StatusCode.UNAVAILABLE:
                self._invalidate_route(topology)
            raise

    @staticmethod
    def _batch_responses(groups, channels, parallel, timeout_seconds):
        options = {} if timeout_seconds is None else {"timeout_seconds": timeout_seconds}
        if not parallel:
            for shard, group in groups.items():
                response = channels[shard].batch_execute(
                    shard.shard_id, (command for _, command in group),
                    load_version=shard.load_version or None,
                    **options,
                )
                yield group, response
            return

        pending = iter(groups.items())
        while window := list(islice(pending, _MAX_INFLIGHT_READ_SHARDS)):
            with ExitStack() as inflight:
                futures = []
                for shard, group in window:
                    future = channels[shard]._start_batch(
                        shard.shard_id, (command for _, command in group),
                        load_version=shard.load_version or None,
                        **options,
                    )
                    inflight.callback(future.cancel)
                    futures.append((group, future))
                for group, future in futures:
                    yield group, future.result()

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
