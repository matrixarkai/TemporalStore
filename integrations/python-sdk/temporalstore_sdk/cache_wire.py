# SPDX-License-Identifier: Apache-2.0
"""Internal cache inserts that keep a borrowed tensor until gRPC serialization."""

from dataclasses import dataclass
from typing import Iterable

from temporalstore.v1 import temporalstore_pb2 as pb


@dataclass(frozen=True, slots=True)
class CacheInsert:
    """Insert an immutable cache value. Keep the C-contiguous buffer valid until the RPC starts."""

    key: str
    value: memoryview
    ttl_ms: int | None


BatchCommand = pb.Command | CacheInsert

_batch = pb.BatchExecuteRequest.DESCRIPTOR.fields_by_name
_insert = pb.StringSetConditional.DESCRIPTOR.fields_by_name
_commands = _batch["commands"].number
_insert_kind = pb.Command.DESCRIPTOR.fields_by_name["string_set_conditional"].number
_key = _insert["key"].number
_value = _insert["value"].number


def _varint(value: int) -> bytes:
    result = bytearray()
    while value > 127:
        result.append(128 | (value & 127))
        value >>= 7
    result.append(value)
    return bytes(result)


def _length_field(number: int, length: int) -> bytes:
    return _varint((number << 3) | 2) + _varint(length)


def encode_cache_batch(
    shard_id: int, commands: Iterable[BatchCommand], load_version: int | None,
) -> bytes:
    # Let protobuf validate the scalar fields. Their encodings are tiny.
    parts = [pb.BatchExecuteRequest(shard_id=shard_id).SerializeToString()]
    for command in commands:
        if not isinstance(command, CacheInsert):
            body = command.SerializeToString()
            parts.extend((_length_field(_commands, len(body)), body))
            continue
        key = command.key.encode("utf-8")
        key_field = (_length_field(_key, len(key)) + key) if key else b""
        flags = pb.StringSetConditional(condition=pb.STRING_SET_CONDITION_IF_NOT_EXISTS)
        if command.ttl_ms is not None:
            flags.ttl_ms = command.ttl_ms
        suffix = flags.SerializeToString()
        value_size = command.value.nbytes
        value_head = _length_field(_value, value_size) if value_size else b""
        insert_size = len(key_field) + len(value_head) + value_size + len(suffix)
        insert_head = _length_field(_insert_kind, insert_size)
        parts.append(_length_field(_commands, len(insert_head) + insert_size)
                     + insert_head + key_field + value_head)
        if value_size:
            parts.append(command.value)
        parts.append(suffix)
    if load_version is not None:
        parts.append(pb.BatchExecuteRequest(load_version=load_version).SerializeToString())
    return b"".join(parts)
