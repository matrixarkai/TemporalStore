# SPDX-License-Identifier: Apache-2.0
"""Internal views over gRPC replies for large string loads."""

from dataclasses import dataclass

from temporalstore.v1 import temporalstore_pb2 as pb


@dataclass(frozen=True, slots=True)
class LoadReply:
    status: pb.Status
    value: memoryview
    value_present: bool


@dataclass(frozen=True, slots=True)
class LoadBatch:
    status: pb.Status
    responses: list[LoadReply]


_batch_fields = pb.BatchExecuteResponse.DESCRIPTOR.fields_by_name
_reply_fields = pb.CommandResponse.DESCRIPTOR.fields_by_name
_batch_status = _batch_fields["status"].number
_responses = _batch_fields["responses"].number
_reply_status = _reply_fields["status"].number
_value = _reply_fields["value"].number
_value_present = _reply_fields["value_present"].number
_result_kind = _reply_fields["result_kind"].number


def _varint(data, offset, end):
    value = 0
    for shift in range(0, 70, 7):
        if offset >= end:
            raise ValueError("truncated protobuf field")
        byte = data[offset]
        offset += 1
        if shift == 63 and byte > 1:
            raise ValueError("protobuf integer exceeds 64 bits")
        value |= (byte & 127) << shift
        if byte < 128:
            return value, offset
    raise ValueError("invalid protobuf integer")


def _length_fields(data, offset, end, *, varint_fields=()):
    while offset < end:
        tag, offset = _varint(data, offset, end)
        number, wire = divmod(tag, 8)
        if number in varint_fields and wire == 0:
            value, offset = _varint(data, offset, end)
            yield number, value
        elif wire == 2:
            length, offset = _varint(data, offset, end)
            limit = offset + length
            if limit > end:
                raise ValueError("truncated length-delimited field")
            yield number, data[offset:limit]
            offset = limit
        else:
            raise ValueError("non-load response field")


def _read_reply(data):
    status = pb.Status()
    value = data[:0]
    present = False
    for field, contents in _length_fields(data, 0, len(data), varint_fields=(_value_present, _result_kind)):
        if field == _reply_status:
            status.MergeFromString(contents)
        elif field == _value:
            value = contents
        elif field == _value_present and isinstance(contents, int):
            present = bool(contents)
        elif field == _result_kind and contents == pb.COMMAND_RESULT_KIND_BYTES:
            continue
        else:
            raise ValueError("non-load response field")
    return LoadReply(status, value, present)


def decode_load_batch(encoded: bytes) -> LoadBatch | pb.BatchExecuteResponse:
    """Use views for ordinary load replies and the generated decoder for other shapes."""
    data = memoryview(encoded)
    status = pb.Status()
    replies = []
    try:
        for field, contents in _length_fields(data, 0, len(data)):
            if field == _batch_status:
                status.MergeFromString(contents)
            elif field == _responses:
                replies.append(_read_reply(contents))
            else:
                raise ValueError("non-load batch field")
    except ValueError:
        return pb.BatchExecuteResponse.FromString(encoded)
    return LoadBatch(status, replies)
