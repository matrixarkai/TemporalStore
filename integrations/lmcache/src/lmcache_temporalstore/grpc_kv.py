# SPDX-License-Identifier: Apache-2.0
"""LMCache's model-layout tables over the shared TemporalStore SDK."""

from __future__ import annotations

import json
import os
from collections.abc import Iterator, Mapping
from hashlib import sha256
from threading import Lock
from time import monotonic, sleep
from typing import TYPE_CHECKING

import grpc
from lmcache.v1.distributed.api import ObjectKey
from temporalstore_sdk import (
    MetadataClient, MetadataError, ProxyCommandClient, ProxyTableClient,
    RaftPrimaryUnavailable, TableClient, commands as pb,
)

from .kv import ValueBackend
from .layout import layout_table_name

if TYPE_CHECKING:
    from lmcache.v1.memory_management import MemoryObjMetadata


TABLE_DESCRIPTOR_KEY = "__lmcache_layout_v1"
# Both codes identify rejection before any work for the affected command.
_STORE_NOT_STARTED = {"raft_write_not_started", "load_version_mismatch"}
_STORE_OUTCOME_UNCERTAIN = {"raft_error", "write_outcome_unknown"}
_STORE_READBACK_ROUTE_CHANGED = {"raft_not_leader", "load_version_mismatch"}
_LOOKUP_TEMPORARILY_UNAVAILABLE = {
    "raft_not_leader", "load_version_mismatch", "no_grpc_primary", "read_unavailable",
}


def _table_for_key(key: ObjectKey) -> str:
    return layout_table_name(key.model_name, (key.kv_rank >> 24) & 255, (key.kv_rank >> 8) & 255)


def _command(document: dict) -> pb.Command:
    kind = document["kind"]
    key = document["key"]
    if kind == "string_set_conditional":
        command = pb.StringSetConditional(
            key=key, value=bytes(document["value"]),
            condition=pb.STRING_SET_CONDITION_IF_NOT_EXISTS,
        )
        if document["ttl_ms"] is not None:
            command.ttl_ms = document["ttl_ms"]
        return pb.Command(string_set_conditional=command)
    if kind == "string_len":
        return pb.Command(string_len=pb.StringLen(key=key))
    if kind == "string_get":
        return pb.Command(string_get=pb.StringGet(key=key))
    if kind == "common_delete":
        return pb.Command(common_delete=pb.CommonDelete(key=key))
    raise ValueError(f"unsupported KV command {kind}")


class TableValueBackend(ValueBackend):
    def __init__(
        self, meta_url: str, namespace: str, timeout_seconds: float,
        lookup_timeout_seconds: float, ttl_seconds: int, *, proxy_grpc_target: str = "",
        store_retry_timeout_seconds: float = 5.0,
    ) -> None:
        self._metadata = MetadataClient(
            meta_url, credential_provider=lambda: os.environ.get("TS_META_ADMIN_TOKEN"),
            require_grpc=True,
            timeout_seconds=min(timeout_seconds, lookup_timeout_seconds),
        )
        self._proxy = ProxyCommandClient(proxy_grpc_target) if proxy_grpc_target else None
        self._namespace = namespace
        self._timeout = timeout_seconds
        self._store_retry_timeout = min(timeout_seconds, store_retry_timeout_seconds)
        self._lookup_timeout = lookup_timeout_seconds
        self._ttl_ms = ttl_seconds * 1000 or None
        self._tables: dict[str, TableClient] = {}
        self._lock = Lock()
        self._stores_confirmed_by_read = 0
        self._registered_layouts: dict[str, tuple[tuple[int | None, int | None], set[tuple]]] = {}

    def _table(self, name: str) -> TableClient:
        with self._lock:
            client = self._tables.get(name)
            if client is None:
                if self._proxy is None:
                    client = TableClient(
                        self._metadata, self._namespace, name, timeout_seconds=self._timeout,
                        parallel_reads=True,
                    )
                else:
                    client = ProxyTableClient(
                        self._metadata, self._proxy, self._namespace, name, timeout_seconds=self._timeout,
                    )
                self._tables[name] = client
            return client

    def register_layouts(self, metadata_by_key: Mapping[ObjectKey, MemoryObjMetadata]) -> None:
        pending_by_table: dict[str, dict[tuple, ObjectKey]] = {}
        for key, metadata in metadata_by_key.items():
            shapes = tuple(tuple(shape) for shape in (metadata.shapes or [metadata.shape]))
            dtypes = tuple(str(dtype) if dtype is not None else None
                           for dtype in (metadata.dtypes or [metadata.dtype]))
            layout = (key.kv_rank, key.object_group_id, shapes, dtypes, metadata.fmt.value)
            pending_by_table.setdefault(_table_for_key(key), {})[layout] = key

        for name, layouts in pending_by_table.items():
            client = self._table(name)
            identity = client.table_identity()
            previous_identity, registered = self._registered_layouts.get(name, (identity, set()))
            if previous_identity != identity:
                registered = set()
            pending = layouts.keys() - registered
            if not pending:
                continue

            fields = []
            for layout in sorted(pending, key=repr):
                rank, group, shapes, dtypes, memory_format = layout
                key = layouts[layout]
                value = json.dumps({
                    "model": key.model_name,
                    "world_size": (rank >> 24) & 255,
                    "local_world_size": (rank >> 8) & 255,
                    "rank": rank,
                    "group": group,
                    "shapes": shapes,
                    "dtypes": dtypes,
                    "memory_format": memory_format,
                }, separators=(",", ":")).encode("utf-8")
                fields.append(pb.HashEntry(field=sha256(value).hexdigest(), value=value))

            command = pb.Command(hash_multi_set=pb.HashMultiSet(key=TABLE_DESCRIPTOR_KEY, entries=fields))
            for _, response in self._store_responses(client, [command]):
                if not response.status.ok:
                    raise MetadataError(response.status.code, response.status.message)
            registered.update(pending)
            self._registered_layouts[name] = identity, registered

    def _iter_responses(self, entries, *, lookup, partial=False):
        groups = {}
        for index, (key, document) in enumerate(entries):
            groups.setdefault(_table_for_key(key), []).append((index, document))
        options = {"timeout_seconds": self._lookup_timeout} if lookup else {}
        for name, group in groups.items():
            client = self._table(name)
            start = 0
            while start < len(group):
                end, bytes_used = start, 0
                commands = []
                for _, document in group[start:start + 128]:
                    size = len(document.get("value", b"")) + len(document["key"].encode("utf-8")) + 64
                    if commands and bytes_used + size > 16 * 1024 * 1024:
                        break
                    commands.append(_command(document))
                    bytes_used += size
                    end += 1
                if partial:
                    responses = self._store_responses(client, commands)
                elif lookup and self._proxy is None:
                    responses = enumerate(self._read_batch(
                        client, commands, dict(options, allow_partial_reads=True),
                    ))
                elif all(command.WhichOneof("kind") in {"string_get", "string_len"} for command in commands):
                    responses = enumerate(self._read_batch(client, commands, options))
                else:
                    responses = enumerate(client.batch_execute(commands, **options))
                for position, response in responses:
                    index, document = group[start + position]
                    yield index, document, response
                start = end

    def _read_batch(self, client, commands, options):
        deadline = monotonic() + options.get("timeout_seconds", self._timeout)
        try:
            responses = client.batch_execute(commands, **options)
        except (MetadataError, grpc.RpcError) as error:
            route_changed = (
                error.code == "raft_not_leader" if isinstance(error, MetadataError)
                else error.code() == grpc.StatusCode.UNAVAILABLE
            )
            remaining = deadline - monotonic()
            if not route_changed or remaining <= 0:
                raise
        else:
            stale = [
                index for index, response in enumerate(responses)
                if response.status.code == "raft_not_leader"
            ]
            if not stale:
                return responses
            remaining = deadline - monotonic()
            if remaining <= 0:
                return responses
            if options.get("allow_partial_reads") or self._proxy is not None:
                try:
                    retry = client.batch_execute(
                        [commands[index] for index in stale],
                        **dict(options, timeout_seconds=remaining),
                    )
                except grpc.RpcError as error:
                    if error.code() in (grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED):
                        return responses
                    raise
                for index, response in zip(stale, retry, strict=True):
                    responses[index] = response
                return responses
        return client.batch_execute(commands, **dict(options, timeout_seconds=remaining))

    def _store_responses(self, client, commands):
        deadline = None
        rejected = None
        pending = dict(enumerate(commands))
        while pending:
            options = {}
            if deadline is not None:
                remaining = deadline - monotonic()
                if remaining <= 0:
                    raise rejected
                options["timeout_seconds"] = remaining
            positions = list(pending)
            rejected = None
            try:
                for position, response in client.iter_batch_responses(list(pending.values()), **options):
                    index = positions[position]
                    if response.status.code in _STORE_NOT_STARTED:
                        rejected = MetadataError(response.status.code, response.status.message)
                    else:
                        del pending[index]
                        yield index, response
            except RaftPrimaryUnavailable as error:
                # TableClient validates every route before dispatching a batch.
                rejected = error
            except MetadataError as error:
                if error.code not in _STORE_NOT_STARTED:
                    raise
                rejected = error
            if not pending:
                return
            if rejected is None:
                raise MetadataError("invalid_batch", "datanode returned incomplete store responses")
            now = monotonic()
            if deadline is None:
                deadline = now + self._store_retry_timeout
            remaining = deadline - now
            if remaining <= 0:
                raise rejected
            sleep(min(0.25, remaining))

    def _execute(self, entries: list[tuple[ObjectKey, dict]], *, lookup: bool = False) -> list[dict]:
        ordered = [None] * len(entries)
        for index, document, response in self._iter_responses(entries, lookup=lookup):
            if not response.status.ok:
                if lookup and response.status.code in _LOOKUP_TEMPORARILY_UNAVAILABLE:
                    ordered[index] = {"value": 0}
                    continue
                raise MetadataError(response.status.code, response.status.message)
            value = (response.value or None) if document["kind"] == "string_get" else response.count
            ordered[index] = {"value": value}
        return ordered

    @property
    def stores_confirmed_by_read(self) -> int:
        with self._lock:
            return self._stores_confirmed_by_read

    def _confirm_stored_value(self, key, stored_key, payload, deadline):
        client = self._table(_table_for_key(key))
        command = _command({"kind": "string_get", "key": stored_key})
        retryable = _STORE_READBACK_ROUTE_CHANGED
        if self._proxy is not None:
            # Proxy versions wrap unavailable data nodes in these status codes.
            retryable = retryable | {"status_error", "server_error"}

        while (remaining := deadline - monotonic()) > 0:
            try:
                replies = client.batch_execute([command], timeout_seconds=remaining)
            except RaftPrimaryUnavailable:
                pass
            except MetadataError as error:
                if error.code not in retryable:
                    return False
            except grpc.RpcError as error:
                if error.code() != grpc.StatusCode.UNAVAILABLE:
                    return False
            else:
                if len(replies) != 1:
                    return False
                reply = replies[0]
                if reply.status.ok:
                    value = reply.value
                    return bool(value) and value == payload
                if reply.status.code not in retryable:
                    return False

            remaining = deadline - monotonic()
            if remaining > 0:
                sleep(min(0.25, remaining))
        return False

    def iter_store_results(
        self, chunks: list[tuple[ObjectKey, memoryview]],
    ) -> Iterator[tuple[int, bool]]:
        failures = []
        entries = self._store_entries(chunks)
        first_error = None
        try:
            for index, document, response in self._iter_responses(entries, lookup=False, partial=True):
                if response.status.ok:
                    yield index, response.count == 1
                else:
                    failures.append((index, document["key"], response.status))
        except (MetadataError, grpc.RpcError) as error:
            # Reconcile replies already received before propagating an interrupted batch.
            first_error = error
        deadline = monotonic() + min(self._timeout, self._lookup_timeout, 5.0) if failures else 0.0
        for index, stored_key, status in failures:
            key, payload = chunks[index]
            if status.code in _STORE_OUTCOME_UNCERTAIN and self._confirm_stored_value(
                key, stored_key, payload, deadline,
            ):
                with self._lock:
                    self._stores_confirmed_by_read += 1
                # The value is usable. The read cannot assign ownership of the insert.
                yield index, False
            elif first_error is None:
                first_error = MetadataError(status.code, status.message)
        if first_error is not None:
            raise first_error

    def close(self) -> None:
        for client in self._tables.values():
            client.close()
        if self._proxy is not None:
            self._proxy.close()
        self._metadata.close()
