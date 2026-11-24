# SPDX-License-Identifier: Apache-2.0
"""Feast commands through the shared table-routed Python SDK."""

import asyncio
import os

from temporalstore_sdk import (
    MetadataClient, TableClient, commands as pb, decode_hash_entries, decode_integer,
    decode_value, decode_values,
)

from .client import TemporalStoreClient, TemporalStoreCommandError, _binary_command


def _entries(fields, values):
    if len(fields) != len(values):
        raise ValueError("feature fields and values must have matching lengths")
    return list(zip(fields, values))


def _command(document):
    fields = _binary_command(document)
    kind = fields.pop("kind")
    if "entries" in fields:
        fields["entries"] = [
            {"field": name, "value": value} for name, value in fields["entries"]
        ]
    if kind not in pb.Command.DESCRIPTOR.fields_by_name:
        raise ValueError(f"command {kind} is unavailable over gRPC")
    return pb.Command(**{kind: fields})


def _response(command, response):
    if not response.status.ok:
        raise TemporalStoreCommandError(response.status.code, response.status.message)
    if response.HasField("values_present"):
        return {"kind": "values", "values": decode_values(response)}
    if response.HasField("value_present"):
        return {"kind": "bytes", "value": decode_value(response)}
    kind = command.WhichOneof("kind")
    if kind == "hash_get_all":
        return {"kind": "hash_entries", "entries": decode_hash_entries(response)}
    if kind in {"feature_agg_query", "feature_proto_agg_query"}:
        return {"kind": "aggregate", "value": decode_integer(response)}
    if response.HasField("integer"):
        return {"kind": "integer", "value": decode_integer(response)}
    return {"kind": "members", "members": list(response.values)}


class TableFeastClient(TemporalStoreClient):
    """Use TemporalStore table placement for Feast reads and writes."""

    def __init__(self, meta_url, namespace, table_name, *limits):
        super().__init__(*limits)
        self._metadata = MetadataClient(
            meta_url, credential_provider=lambda: os.environ.get("TS_META_ADMIN_TOKEN"),
            timeout_seconds=self.timeout_seconds,
        )
        self._table = TableClient(
            self._metadata, namespace, table_name, timeout_seconds=self.timeout_seconds,
        )
        self._interleaved_conditional_write_supported = False

    def _groups(self, commands, max_commands):
        group, size = [], 0
        for command in commands:
            command_bytes = command.ByteSize() + 8
            if group and (len(group) == max_commands or
                          size + command_bytes > self.max_batch_request_bytes):
                yield group
                group, size = [], 0
            group.append(command)
            size += command_bytes
        if group:
            yield group

    def _batch(self, documents, max_commands):
        commands = [_command(document) for document in documents]
        return [
            _response(command, response)
            for group in self._groups(commands, max_commands)
            for command, response in zip(group, self._table.batch_execute(group))
        ]

    def batch_execute(self, commands):
        return self._batch(commands, self.max_batch_commands)

    def execute(self, command):
        return self.batch_execute([command])[0]

    def batch_write(self, commands):
        self._batch(commands, self.max_write_batch_commands)

    def _projection(self, keys, fields):
        return [
            pb.Command(hash_multi_get=pb.HashMultiGet(key=key, fields=fields)) for key in keys
        ]

    def _read_projection(self, commands):
        rows = []
        for response in self._table.batch_execute(commands):
            if not response.status.ok:
                raise TemporalStoreCommandError(response.status.code, response.status.message)
            rows.append(decode_values(response))
        return rows

    def batch_hash_multi_get(self, keys, fields):
        return [
            row
            for group in self._groups(self._projection(keys, fields), self.max_batch_commands)
            for row in self._read_projection(group)
        ]

    async def batch_hash_multi_get_async(self, keys, fields):
        groups = list(self._groups(self._projection(keys, fields), self.max_batch_commands))
        results = []
        for start in range(0, len(groups), self.max_async_read_batches):
            window = groups[start:start + self.max_async_read_batches]
            for rows in await asyncio.gather(
                *(asyncio.to_thread(self._read_projection, group) for group in window)
            ):
                results.extend(rows)
        return results

    def batch_hash_multi_set(self, fields, rows, request_upper_bound=None, ttl_ms=None):
        kind = "hash_multi_set_with_ttl" if ttl_ms is not None else "hash_multi_set"
        self.batch_write([
            {
                "kind": kind, "key": key, "entries": _entries(fields, values),
                **({"ttl_ms": ttl_ms} if ttl_ms is not None else {}),
            }
            for key, values in rows
        ])

    def batch_hash_timestamp_set(
        self, fields, timestamp_field, rows, request_upper_bound=None, ttl_ms=None,
    ):
        responses = self._batch([
            {
                "kind": "hash_multi_set_if_timestamp_older", "key": key,
                "timestamp_field": timestamp_field, "timestamp_seconds": seconds,
                "timestamp_nanos": nanos, "entries": _entries(fields, values),
                **({"ttl_ms": ttl_ms} if ttl_ms is not None else {}),
            }
            for key, seconds, nanos, values in rows
        ], self.max_write_batch_commands)
        applied = sum(response["value"] == 1 for response in responses)
        return applied, len(responses) - applied

    def close(self):
        self._table.close()
        self._metadata.close()
        super().close()
