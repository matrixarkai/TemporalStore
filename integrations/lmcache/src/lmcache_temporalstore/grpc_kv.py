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
from temporalstore_sdk.cache_wire import CacheInsert
from temporalstore_sdk.client import write_outcome_is_unknown

from .kv import ValueBackend
from .layout import layout_table_name

if TYPE_CHECKING:
    from lmcache.v1.memory_management import MemoryObjMetadata


TABLE_DESCRIPTOR_KEY = "__lmcache_layout_v1"
_BORROWED_INSERT_MIN_BYTES = 256 * 1024
_BORROWED_LOAD_MIN_BYTES = 256 * 1024
# Both codes identify rejection before any work for the affected command.
_STORE_NOT_STARTED = {"raft_write_not_started", "load_version_mismatch"}
_STORE_OUTCOME_UNCERTAIN = {"raft_error", "write_outcome_unknown"}
_READ_ROUTE_CHANGED = {"raft_not_leader", "load_version_mismatch"}
_READ_RETRYABLE_STATUS = _READ_ROUTE_CHANGED | {"read_unavailable"}
_LOOKUP_TEMPORARILY_UNAVAILABLE = {
    "raft_not_leader", "load_version_mismatch", "no_grpc_primary", "read_unavailable",
}


def _table_for_key(key: ObjectKey) -> str:
    return layout_table_name(key.model_name, (key.kv_rank >> 24) & 255, (key.kv_rank >> 8) & 255)


def _command(document: dict, *, borrow_payload: bool = False) -> pb.Command | CacheInsert:
    kind = document["kind"]
    key = document["key"]
    if kind == "string_set_conditional":
        if borrow_payload:
            tensor = memoryview(document["value"])
            if tensor.nbytes >= _BORROWED_INSERT_MIN_BYTES and tensor.c_contiguous:
                return CacheInsert(key, tensor, document["ttl_ms"])
        command = pb.Command()
        set_value = command.string_set_conditional
        set_value.key = key
        set_value.value = bytes(document["value"])
        set_value.condition = pb.STRING_SET_CONDITION_IF_NOT_EXISTS
        if document["ttl_ms"] is not None:
            set_value.ttl_ms = document["ttl_ms"]
        return command
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
                        parallel_reads=True, preserve_insert_outcomes=True,
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

    def _iter_responses(self, entries, *, lookup, partial=False, borrowed_load=False):
        groups = {}
        for index, (key, document) in enumerate(entries):
            groups.setdefault(_table_for_key(key), []).append((index, document))
        options = {"timeout_seconds": self._lookup_timeout} if lookup else {}
        if borrowed_load:
            options["borrowed_load"] = True
        for name, group in groups.items():
            client = self._table(name)
            start = 0
            while start < len(group):
                end, bytes_used = start, 0
                commands = []
                for _, document in group[start:start + 128]:
                    value = document.get("value", b"")
                    value_size = value.nbytes if isinstance(value, memoryview) else len(value)
                    size = value_size + len(document["key"].encode("utf-8")) + 64
                    if commands and bytes_used + size > 16 * 1024 * 1024:
                        break
                    commands.append(_command(document, borrow_payload=partial and self._proxy is None))
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
                error.code in _READ_RETRYABLE_STATUS if isinstance(error, MetadataError)
                else error.code() == grpc.StatusCode.UNAVAILABLE
            )
            remaining = deadline - monotonic()
            if not route_changed or remaining <= 0:
                raise
        else:
            stale = [
                index for index, response in enumerate(responses)
                if response.status.code in _READ_RETRYABLE_STATUS
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
                except MetadataError as error:
                    if error.code in {"unavailable", "deadline_exceeded"}:
                        return responses
                    raise
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

    def load_into(self, items: list[tuple[ObjectKey, memoryview]]) -> Iterator[bool]:
        for batch, entries in self._load_batches(items, pair_large=self._proxy is None):
            responses = [None] * len(batch)
            borrowed_load = self._proxy is None and any(
                destination.nbytes >= _BORROWED_LOAD_MIN_BYTES for _, destination in batch
            )
            options = {"borrowed_load": True} if borrowed_load else {}
            for index, _, response in self._iter_responses(entries, lookup=False, **options):
                if not response.status.ok:
                    raise MetadataError(response.status.code, response.status.message)
                responses[index] = response
            if any(response is None for response in responses):
                raise MetadataError("invalid_batch", "datanode returned incomplete load responses")
            # Large gRPC replies expose slices of one immutable response buffer.
            for (_, destination), response in zip(batch, responses, strict=True):
                payload = response.value
                found = bool(payload) and len(payload) == len(destination)
                if found:
                    destination[:] = payload
                del payload, response
                yield found
            del responses

    @property
    def stores_confirmed_by_read(self) -> int:
        with self._lock:
            return self._stores_confirmed_by_read

    def _confirm_stored_value(self, key, stored_key, payload, deadline):
        client = self._table(_table_for_key(key))
        command = _command({"kind": "string_get", "key": stored_key})
        retryable = _READ_ROUTE_CHANGED
        if self._proxy is not None:
            # Proxy versions wrap unavailable data nodes in these status codes.
            retryable = retryable | {"status_error", "server_error"}

        recheck_missing = True
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
                    if value or reply.value_present:
                        return bool(value) and value == payload
                    if not recheck_missing:
                        return False
                    # A new leader may not have applied the uncertain write yet.
                    # Recheck once within the existing deadline without writing again.
                    recheck_missing = False
                elif reply.status.code not in retryable:
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
        replied = set()
        first_error = None
        transport_error = None
        try:
            for index, document, response in self._iter_responses(entries, lookup=False, partial=True):
                replied.add(index)
                if response.status.ok:
                    yield index, response.count == 1
                else:
                    status = response.status
                    failures.append((
                        index, document["key"], status.code in _STORE_OUTCOME_UNCERTAIN,
                        MetadataError(status.code, status.message),
                    ))
        except grpc.RpcError as error:
            if len(replied) < len(entries) and write_outcome_is_unknown(error):
                transport_error = error
                failures.extend(
                    (index, document["key"], True, error)
                    for index, (_, document) in enumerate(entries) if index not in replied
                )
            else:
                first_error = error
        except MetadataError as error:
            first_error = error

        waiting = sum(uncertain for _, _, uncertain, _ in failures)
        deadline = monotonic() + min(self._timeout, self._lookup_timeout, 5.0) if waiting else 0.0
        for index, stored_key, uncertain, error in failures:
            if uncertain:
                key, payload = chunks[index]
                read_deadline = deadline
                if waiting > 1:
                    # Leave time to confirm writes whose shards already recovered.
                    now = min(monotonic(), deadline)
                    read_deadline = now + (deadline - now) / waiting
                waiting -= 1
                if self._confirm_stored_value(key, stored_key, payload, read_deadline):
                    with self._lock:
                        self._stores_confirmed_by_read += 1
                    # The value is usable. The read cannot assign ownership of the insert.
                    yield index, False
                    continue
            if first_error is None or error is transport_error:
                first_error = error
        if first_error is not None:
            raise first_error

    def close(self) -> None:
        for client in self._tables.values():
            client.close()
        if self._proxy is not None:
            self._proxy.close()
        self._metadata.close()
