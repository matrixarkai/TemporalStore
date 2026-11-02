# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
r"""Byte-safe encoding for keys and hash fields on the TemporalStore RESP surface.

THIS IS NOT THE INTENDED WIRE FORMAT. IT IS A WORKAROUND FOR A SERVER DEFECT, AND IT IS MEANT
TO BE DELETED.

The cause is named and it is NOT a small patch. ``string_arg`` is
``String::from_utf8_lossy(value).to_string()`` at ``redis/response_helpers.rs:115``, reached from
149 call sites across the RESP dispatch -- ``HSET``, ``HMGET``, ``HKEYS``, ``HDEL``, ``DEL`` and
``EXPIRE`` among them. But it is not merely a dispatch-local conversion: the engine's own command
type declares those positions as text, ::

    Command::HashSet { key: String, field: String, value: Vec<u8> }

where only ``value`` carries the bytes codec (``#[serde(with = "crate::bytes_serde")]``). That
type is what is serialised into the write-ahead log and replicated, so a key or field cannot be
made binary without changing the DURABLE command encoding -- which needs a format version and a
reader-first rollout, not a one-line change. The workaround in this module is what makes feature
serving correct in the meantime.

THE DELETION CONDITION, BOTH HALVES REQUIRED. This module can be dropped only once the server
guarantees **both** that ``HKEYS``/``HGETALL`` round-trip arbitrary byte names back identically,
**and** that two distinct byte names never collide. A fix that stops the collision while still
handing back a mangled name does not free this code: ``online_store._drop_view`` reads field names
back and must match them against the ones it wrote. Nothing else in the integration depends on
the encoded shape -- stored rows are addressed only through
:mod:`feast_temporalstore.online_store` -- so when both hold, the change is this file, its tests,
and a re-materialization of existing rows. Until then the encoding is load-bearing, and
``tests/test_wire.py::test_raw_field_names_collide_under_the_server_and_encoded_ones_do_not``
fails with instructions if anyone removes it while the cause is still present.

WHY IT IS NEEDED AT ALL, MEASURED RATHER THAN ASSUMED.

Feast addresses an online row by a BINARY key and a BINARY hash field:

  * ``_redis_key(project, entity_key)`` returns ``serialize_entity_key(entity_key) +
    project.encode("utf-8")``. The serialized entity key is a packed binary structure, so the
    key is arbitrary bytes.
  * ``_mmh3(f"{view}:{feature}")`` returns EXACTLY FOUR arbitrary bytes -- the little-endian
    murmur3_32 of the name.

The TemporalStore RESP dispatch decodes both with ``String::from_utf8_lossy`` (``string_arg``),
which replaces every invalid byte run with U+FFFD. Values are untouched and stay binary.

Two figures, both computed rather than guessed:

  * **91.0763%** of 4-byte strings are not valid UTF-8 (exact, by dynamic programming over the
    byte alphabet, agreeing with an exhaustive check of all 2**24 three-byte strings). So the
    overwhelming majority of Feast field names do not survive the trip.
  * Two distinct 4-byte field names collide after the lossy conversion with probability
    **1 in 1,042** (4,000,000 samples), against **1 in 4,294,967,296** for a correct store --
    a four-million-fold increase. 2.89% of all possible field names collapse onto the single
    string ``'\ufffd\ufffd\ufffd\ufffd'``. A feature view with 50 features has a 69% chance of
    at least one collision; with 100 features, 99.1%.

A collision is not a slow answer, it is a wrong one: two features share a field, one overwrites
the other, and ``online_read`` returns the wrong feature's value. So this module encodes both
halves into byte ranges that survive the conversion unchanged.

WHY TWO DIFFERENT ENCODINGS, which is a deliberate asymmetry and not an oversight:

  * **Keys use hex.** ``SCAN MATCH`` patterns are built by prefix and suffix, and hex is a fixed
    two-characters-per-byte homomorphism, so a byte prefix maps to a character prefix and a byte
    suffix to a character suffix. Base64 does not have that property except at three-byte
    boundaries, which would break pattern matching on an arbitrary prefix.
  * **Fields use base64url without padding.** A field is never pattern-matched -- it is written
    whole, read whole by ``HKEYS``, and deleted whole by ``HDEL`` -- so the prefix property buys
    nothing and the smaller form wins: a 4-byte field is 6 characters instead of 8.

Both output alphabets are free of the glob metacharacters ``*``, ``?``, ``[``, ``]`` and ``\``,
so an encoded key can be embedded in a ``SCAN MATCH`` pattern without quoting.

THE COST, STATED PLAINLY. Hex doubles the stored key length and base64url grows a field by a
third. That is a real footprint cost of the integration, it is the price of correctness on the
surface as it stands, and it is the second reason to fix the dispatch rather than live with this
module: fixing it recovers the bytes as well as the correctness.
"""

import base64

__all__ = [
    "encode_key",
    "decode_key",
    "encode_field",
    "decode_field",
    "encode_scan_pattern",
]


def encode_key(raw: bytes) -> bytes:
    """A binary row key as lowercase hex, which preserves byte prefixes and suffixes."""
    if not isinstance(raw, (bytes, bytearray)):
        raise TypeError(f"a row key must be bytes, got {type(raw).__name__}")
    return bytes(raw).hex().encode("ascii")


def decode_key(encoded: bytes) -> bytes:
    """Inverse of :func:`encode_key`."""
    return bytes.fromhex(encoded.decode("ascii"))


def encode_field(raw: bytes) -> bytes:
    """A binary hash field as unpadded base64url.

    The padding is stripped because ``=`` carries no information here (the length is implied by
    the string) and a shorter field is a smaller stored row.
    """
    if not isinstance(raw, (bytes, bytearray)):
        raise TypeError(f"a hash field must be bytes, got {type(raw).__name__}")
    return base64.urlsafe_b64encode(bytes(raw)).rstrip(b"=")


def decode_field(encoded: bytes) -> bytes:
    """Inverse of :func:`encode_field`, restoring the padding base64 needs to decode."""
    padding = b"=" * (-len(encoded) % 4)
    return base64.urlsafe_b64decode(bytes(encoded) + padding)


def encode_scan_pattern(prefix: bytes = b"", suffix: bytes = b"") -> bytes:
    """A ``SCAN MATCH`` pattern over the ENCODED keyspace.

    Correct because hex is two characters per byte: the hex of a byte prefix is a character
    prefix of the hex of the whole key, and likewise for a suffix. The ``*`` stands for an
    unknown whole number of bytes, which is an even number of hex characters -- and since the
    glob ``*`` matches any run of characters including an odd one, the pattern is permissive
    rather than wrong: it can match more than intended only if the keyspace holds odd-length hex,
    which :func:`encode_key` never produces.
    """
    return encode_key(prefix) + b"*" + encode_key(suffix)
