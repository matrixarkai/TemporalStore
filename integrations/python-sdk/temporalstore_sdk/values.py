# SPDX-License-Identifier: Apache-2.0
"""Decode optional binary values without treating empty values as misses."""

from typing import Optional

from temporalstore.v1 import temporalstore_pb2 as pb


def decode_value(response: pb.CommandResponse) -> Optional[bytes]:
    if not response.HasField("value_present"):
        raise ValueError("response does not carry value presence")
    return response.value if response.value_present else None


def decode_values(response: pb.CommandResponse) -> list[Optional[bytes]]:
    if not response.HasField("values_present"):
        raise ValueError("response does not carry projection presence")
    bitmap = response.values_present
    if len(bitmap) != (len(response.values) + 7) // 8:
        raise ValueError("response has an invalid projection presence bitmap")
    return [
        value if bitmap[index // 8] & (1 << (index % 8)) else None
        for index, value in enumerate(response.values)
    ]


def decode_hash_entries(response: pb.CommandResponse) -> list[tuple[str, bytes]]:
    if len(response.entry_names) != len(response.values):
        raise ValueError("response has a different number of field names and values")
    return list(zip(response.entry_names, response.values))


def decode_integer(response: pb.CommandResponse) -> int:
    if not response.HasField("integer"):
        raise ValueError("response does not carry a signed integer")
    return response.integer
