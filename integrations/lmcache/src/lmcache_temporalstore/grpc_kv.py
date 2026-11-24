# SPDX-License-Identifier: Apache-2.0
"""LMCache's model-layout tables over the shared TemporalStore SDK."""

import os
from threading import Lock

import grpc
from lmcache.v1.distributed.api import ObjectKey
from temporalstore_sdk import MetadataClient, MetadataError, TableClient, commands as pb

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


def _execute_batch(client: TableClient, commands: list[pb.Command]) -> list[pb.CommandResponse]:
    try:
        return client.batch_execute(commands)
    except grpc.RpcError as error:
        if (error.code() != grpc.StatusCode.RESOURCE_EXHAUSTED
                or len(commands) < 2
                or not all(command.HasField("string_get") for command in commands)):
            raise
        # Actual stored bytes can exceed the destination size used for batching.
        # Retry only the reads and leave the load path to check each value's size.
        midpoint = len(commands) // 2
        return _execute_batch(client, commands[:midpoint]) + _execute_batch(client, commands[midpoint:])


class TableValueBackend(ValueBackend):
    # Leave space for the protobuf envelope below the transport's 32 MiB limit.
    _max_load_payload_bytes = 24 * 1024 * 1024

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

    def _execute(self, entries: list[tuple[ObjectKey, dict]], *, lookup: bool = False) -> list[dict]:
        groups = {}
        for index, (key, document) in enumerate(entries):
            groups.setdefault(_table_for_key(key), []).append((index, document))
        ordered = [None] * len(entries)
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
                responses = _execute_batch(client, commands)
                for (index, document), response in zip(group[start:end], responses, strict=True):
                    if not response.status.ok:
                        raise MetadataError(response.status.code, response.status.message)
                    value = (response.value or None) if document["kind"] == "string_get" else response.count
                    ordered[index] = {"value": value}
                start = end
        return ordered

    def close(self) -> None:
        for client in self._tables.values():
            client.close()
