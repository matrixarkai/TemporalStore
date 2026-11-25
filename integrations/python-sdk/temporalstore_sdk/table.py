# SPDX-License-Identifier: Apache-2.0
"""Table-scoped data routing through the metaserver."""

from contextlib import closing, contextmanager, ExitStack
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
_ROUTE_ERROR_CODES = {"load_version_mismatch", "raft_error", "raft_write_not_started", "raft_not_leader"}

# The KV and Feast read paths can overlap independent shards. Writes and other
# commands retain their existing dispatch order.
_PARALLEL_READ_KINDS = frozenset({
    "common_exists", "common_ttl", "feature_agg_query", "feature_proto_agg_query",
    "hash_get", "hash_get_all", "hash_multi_get", "string_get", "string_get_range",
    "string_len",
})
_READ_ROUTE_ERRORS = frozenset({"load_version_mismatch", "raft_not_leader"})
_INSERT_ROUTE_ERRORS = frozenset({
    "load_version_mismatch", "raft_write_not_started", "raft_error", "write_outcome_unknown",
})
_TRANSIENT_RPC_ERRORS = frozenset({grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED})
_MAX_INFLIGHT_READ_SHARDS = 8




def _remaining_read_timeout(deadline: float) -> float:
    remaining = deadline - monotonic()
    if remaining <= 0:
        raise MetadataError("deadline_exceeded", "table read deadline expired")
    return remaining


def _undispatched_reads(group) -> pb.BatchExecuteResponse:
    status = pb.Status(code="read_unavailable", message="table read deadline expired before dispatch")
    return pb.BatchExecuteResponse(
        responses=(pb.CommandResponse(status=status) for _ in group),
    )


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
        preserve_insert_outcomes: bool = False,
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
        self._preserve_insert_outcomes = preserve_insert_outcomes
        self._topology = None
        self._expires_at = 0.0
        self._channels: dict[str, CommandClient] = {}
        self._lock = Lock()
        self._closed = False

    @contextmanager
    def _timed_route_lock(self, deadline: float):
        if not self._lock.acquire(timeout=_remaining_read_timeout(deadline)):
            raise MetadataError("deadline_exceeded", "table routing wait exceeded the read deadline")
        try:
            yield
        finally:
            self._lock.release()

    def _current_topology(self, deadline: float | None = None) -> TableTopology:
        with self._lock if deadline is None else self._timed_route_lock(deadline):
            if self._closed:
                raise RuntimeError("table client is closed")
            if self._topology is None or monotonic() >= self._expires_at:
                options = {} if deadline is None else {"timeout_seconds": _remaining_read_timeout(deadline)}
                self._topology = self._metadata.table_topology(self._namespace, self._table, **options)
                ttl = self._topology_ttl
                if any(shard.uses_raft_route and not shard.primary_grpc for shard in self._topology.shards):
                    ttl = min(ttl, _ELECTION_TOPOLOGY_TTL_SECONDS)
                self._expires_at = monotonic() + ttl
            return self._topology

    def table_identity(self) -> tuple[int | None, int | None]:
        """Identify this table across route changes and distinguish a recreated table."""
        topology = self._current_topology()
        if topology.table_id is None:
            return None, topology.version
        return topology.table_id, None

    @staticmethod
    def _shard_for_command(topology: TableTopology, command: pb.Command) -> Shard:
        kind = command.WhichOneof("kind")
        body = getattr(command, kind) if kind is not None else None
        key = getattr(body, "key", None)
        if not isinstance(key, str):
            raise ValueError("table routing requires a single-key command")
        return topology.shard_for_key(key)

    def _channel_for(self, shard: Shard, deadline: float | None = None) -> CommandClient:
        if not shard.primary_grpc:
            if shard.uses_raft_route and shard.primary is None:
                raise RaftPrimaryUnavailable(shard.shard_id)
            raise MetadataError("no_grpc_primary", f"shard {shard.shard_id} has no gRPC primary")
        with self._lock if deadline is None else self._timed_route_lock(deadline):
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
        is_read = command.WhichOneof("kind") in _PARALLEL_READ_KINDS
        deadline = monotonic() + self._timeout if is_read else None
        for attempt in range(2 if is_read else 1):
            topology = self._current_topology(deadline)
            shard = self._shard_for_command(topology, command)
            channel = self._channel_for(shard, deadline)
            options = {} if deadline is None else {"timeout_seconds": _remaining_read_timeout(deadline)}
            try:
                response = channel.execute(shard.shard_id, command,
                                           load_version=shard.load_version or None, **options)
            except grpc.RpcError as error:
                if error.code() != grpc.StatusCode.UNAVAILABLE:
                    raise
                self._invalidate_route(topology)
                if not is_read or attempt or monotonic() >= deadline:
                    raise
                continue
            if response.status.code in _ROUTE_ERROR_CODES:
                self._invalidate_route(topology)
                if is_read and not attempt and response.status.code in _READ_ROUTE_ERRORS and monotonic() < deadline:
                    continue
            return response

    def batch_execute(
        self, commands: Iterable[pb.Command], *, timeout_seconds: float | None = None,
        allow_partial_reads: bool = False,
    ) -> list[pb.CommandResponse]:
        """Return results in request order.

        A read-only batch can opt in to per-shard errors for temporary routing
        and transport failures. Other failures are propagated to the caller.
        An explicit read timeout is shared by route discovery and shard RPCs.
        """
        options = {"allow_partial_reads": True} if allow_partial_reads else {}
        indexed = sorted(
            self.iter_batch_responses(commands, timeout_seconds=timeout_seconds, **options),
            key=lambda item: item[0],
        )
        return [result for _, result in indexed]

    def iter_batch_responses(
        self, commands: Iterable[pb.Command], *, timeout_seconds: float | None = None,
        allow_partial_reads: bool = False,
    ) -> Iterator[tuple[int, pb.CommandResponse]]:
        """Yield indexed shard results, including opted-in read or insert failures."""
        commands = list(commands)
        if not commands:
            return
        read_only = all(command.WhichOneof("kind") in _PARALLEL_READ_KINDS for command in commands)
        if allow_partial_reads and not read_only:
            raise ValueError("partial results require a read-only batch")
        deadline = None
        if read_only and timeout_seconds is not None:
            if not isfinite(timeout_seconds) or timeout_seconds <= 0:
                raise ValueError("read timeout must be finite and positive")
            deadline = monotonic() + timeout_seconds
        topology = self._current_topology(deadline)
        groups: dict[Shard, list[tuple[int, pb.Command]]] = {}
        for position, command in enumerate(commands):
            shard = self._shard_for_command(topology, command)
            groups.setdefault(shard, []).append((position, command))
        # A write batch validates all routes before it can dispatch any command.
        channels = {}
        missing = []
        for shard, group in groups.items():
            try:
                channels[shard] = self._channel_for(shard, deadline)
            except RaftPrimaryUnavailable as error:
                if not allow_partial_reads:
                    raise
                status = pb.Status(code=error.code, message=str(error))
                missing.extend((position, pb.CommandResponse(status=status)) for position, _ in group)
        if missing:
            groups = {shard: groups[shard] for shard in channels}
            yield from missing
        recoverable_inserts = self._preserve_insert_outcomes and all(
            command.HasField("string_set_conditional")
            and command.string_set_conditional.condition == pb.STRING_SET_CONDITION_IF_NOT_EXISTS
            for command in commands
        )
        parallel = len(groups) > 1 and self._parallel_reads and read_only
        try:
            responses = self._batch_responses(
                groups, channels, parallel, timeout_seconds, allow_partial_reads or recoverable_inserts, deadline,
            )
            with closing(responses) as batches:
                for group, response in batches:
                    if isinstance(response, grpc.RpcError):
                        if response.code() == grpc.StatusCode.UNAVAILABLE:
                            self._invalidate_route(topology)
                        if response.code() not in _TRANSIENT_RPC_ERRORS:
                            raise response
                        code = "write_outcome_unknown" if recoverable_inserts else "read_unavailable"
                        status = pb.Status(code=code, message=response.details() or "")
                    elif len(response.responses) != len(group):
                        status = (response.status if not response.status.ok else pb.Status(
                            code="invalid_batch", message="datanode returned the wrong response count",
                        ))
                        if status.code in _ROUTE_ERROR_CODES:
                            self._invalidate_route(topology)
                        if not ((allow_partial_reads and status.code in _READ_ROUTE_ERRORS)
                                or (recoverable_inserts and status.code in _INSERT_ROUTE_ERRORS)):
                            raise MetadataError(status.code, status.message)
                    else:
                        status = None
                        if response.status.code in _ROUTE_ERROR_CODES:
                            self._invalidate_route(topology)
                    results = (
                        response.responses if status is None
                        else (pb.CommandResponse(status=status) for _ in group)
                    )
                    for (position, _), result in zip(group, results):
                        if result.status.code in _ROUTE_ERROR_CODES:
                            self._invalidate_route(topology)
                        yield position, result
        except grpc.RpcError as error:
            if error.code() == grpc.StatusCode.UNAVAILABLE:
                self._invalidate_route(topology)
            raise

    @staticmethod
    def _batch_responses(groups, channels, parallel, timeout_seconds, preserve_rpc_error, deadline):
        options = {} if timeout_seconds is None else {"timeout_seconds": timeout_seconds}
        if not parallel:
            for shard, group in groups.items():
                if deadline is not None:
                    try:
                        options["timeout_seconds"] = _remaining_read_timeout(deadline)
                    except MetadataError:
                        if not preserve_rpc_error:
                            raise
                        yield group, _undispatched_reads(group)
                        continue
                try:
                    response = channels[shard].batch_execute(
                        shard.shard_id, (command for _, command in group),
                        load_version=shard.load_version or None,
                        **options,
                    )
                except grpc.RpcError as error:
                    if not preserve_rpc_error:
                        raise
                    response = error
                yield group, response
            return

        pending = iter(groups.items())
        while window := list(islice(pending, _MAX_INFLIGHT_READ_SHARDS)):
            with ExitStack() as inflight:
                futures = []
                pending_deadline = []
                for shard, group in window:
                    if deadline is not None:
                        try:
                            options["timeout_seconds"] = _remaining_read_timeout(deadline)
                        except MetadataError:
                            if not preserve_rpc_error:
                                raise
                            pending_deadline.append(group)
                            continue
                    future = channels[shard]._start_batch(
                        shard.shard_id, (command for _, command in group),
                        load_version=shard.load_version or None,
                        **options,
                    )
                    inflight.callback(future.cancel)
                    futures.append((group, future))
                for group, future in futures:
                    try:
                        response = future.result()
                    except grpc.RpcError as error:
                        if not preserve_rpc_error:
                            raise
                        response = error
                    yield group, response
                for group in pending_deadline:
                    yield group, _undispatched_reads(group)

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
