# SPDX-License-Identifier: Apache-2.0
"""LMCache's model-layout tables over the shared TemporalStore SDK."""

import os
from collections.abc import Iterator
from threading import Lock
from time import monotonic, sleep

from lmcache.v1.distributed.api import ObjectKey
from temporalstore_sdk import MetadataClient, MetadataError, RaftPrimaryUnavailable, TableClient, commands as pb

from .kv import ValueBackend
from .layout import layout_table_name


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
        lookup_timeout_seconds: float, ttl_seconds: int,
    ) -> None:
        self._metadata = MetadataClient(
            meta_url, credential_provider=lambda: os.environ.get("TS_META_ADMIN_TOKEN"),
            timeout_seconds=min(timeout_seconds, lookup_timeout_seconds),
        )
        self._namespace = namespace
        self._timeout = timeout_seconds
        self._lookup_timeout = lookup_timeout_seconds
        self._ttl_ms = ttl_seconds * 1000 or None
        self._tables: dict[tuple[str, float], TableClient] = {}
        self._lock = Lock()

    def _table(self, name: str, timeout: float) -> TableClient:
        identity = (name, timeout)
        with self._lock:
            client = self._tables.get(identity)
            if client is None:
                client = TableClient(self._metadata, self._namespace, name, timeout_seconds=timeout)
                self._tables[identity] = client
            return client

    def _iter_responses(self, entries, *, lookup, partial=False):
        groups = {}
        for index, (key, document) in enumerate(entries):
            groups.setdefault(_table_for_key(key), []).append((index, document))
        timeout = self._lookup_timeout if lookup else self._timeout
        for name, group in groups.items():
            client = self._table(name, timeout)
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
                responses = (self._store_responses(client, commands) if partial
                             else enumerate(client.batch_execute(commands)))
                for position, response in responses:
                    index, document = group[start + position]
                    yield index, document, response
                start = end

    def _store_responses(self, client, commands):
        deadline = monotonic() + min(self._timeout, 5.0)
        while True:
            try:
                yield from client.iter_batch_responses(commands)
                return
            except RaftPrimaryUnavailable:
                # TableClient validates every route before dispatching this batch.
                remaining = deadline - monotonic()
                if remaining <= 0:
                    raise
                sleep(min(0.25, remaining))

    def _execute(self, entries: list[tuple[ObjectKey, dict]], *, lookup: bool = False) -> list[dict]:
        ordered = [None] * len(entries)
        for index, document, response in self._iter_responses(entries, lookup=lookup):
            if not response.status.ok:
                raise MetadataError(response.status.code, response.status.message)
            value = (response.value or None) if document["kind"] == "string_get" else response.count
            ordered[index] = {"value": value}
        return ordered

    def iter_store_results(
        self, chunks: list[tuple[ObjectKey, memoryview]],
    ) -> Iterator[tuple[int, bool]]:
        failure = None
        for index, _, response in self._iter_responses(self._store_entries(chunks), lookup=False, partial=True):
            if response.status.ok:
                yield index, response.count == 1
            elif failure is None:
                failure = MetadataError(response.status.code, response.status.message)
        if failure is not None:
            raise failure

    def close(self) -> None:
        for client in self._tables.values():
            client.close()
