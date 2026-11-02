# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""The encoding is the whole correctness argument, so it is tested without a server.

These tests need neither Feast nor a running store: they model the one behaviour of the RESP
dispatch that forces the encoding to exist, and they fail if the encoding stops defending
against it.
"""

import random

from feast_temporalstore.wire import (
    decode_field,
    decode_key,
    encode_field,
    encode_key,
    encode_scan_pattern,
)


def lossy(raw: bytes) -> str:
    """What the RESP dispatch does to a key or field argument.

    ``String::from_utf8_lossy`` in Rust and ``bytes.decode('utf-8', 'replace')`` in Python both
    implement Unicode maximal-subpart replacement, so this models the server faithfully.
    """
    return raw.decode("utf-8", "replace")


def test_the_wire_layer_imports_without_feast_or_pydantic():
    """THE CLAIM THAT THESE TESTS NEED NO FEAST, PINNED.

    It was false once already: adding ``from .online_store import ...`` to the package
    ``__init__`` made every test here depend on pydantic, and the suite went from passing to
    failing at import. ``__init__`` resolves the public names lazily (PEP 562) so that cannot
    happen silently again. A test that merely imported ``wire`` would not catch it, because the
    package ``__init__`` runs first either way -- so this asserts on ``sys.modules``.
    """
    import sys

    import feast_temporalstore  # noqa: F401  -- the package __init__ is what is under test
    import feast_temporalstore.wire  # noqa: F401

    for heavy in ("feast", "pydantic"):
        assert heavy not in sys.modules, (
            f"importing feast_temporalstore pulled in {heavy!r}. The package __init__ must resolve "
            "its public names lazily, or the encoding's tests stop being runnable without Feast's "
            "whole stack installed."
        )


def test_a_key_of_arbitrary_bytes_survives_the_round_trip():
    # Every byte value, including the NUL and the continuation bytes that break UTF-8.
    raw = bytes(range(256))
    assert decode_key(encode_key(raw)) == raw


def test_a_four_byte_field_survives_the_round_trip():
    for value in range(0, 1 << 16):
        raw = value.to_bytes(4, "little")
        assert decode_field(encode_field(raw)) == raw


def test_an_encoded_name_is_unchanged_by_the_lossy_decode_the_server_applies():
    """The point of the encoding: the server's conversion becomes the identity."""
    raw = bytes(range(256))
    encoded = encode_key(raw)
    assert lossy(encoded).encode("utf-8") == encoded
    field = encode_field(b"\xff\xfe\xfd\xfc")
    assert lossy(field).encode("utf-8") == field


def test_raw_field_names_collide_under_the_server_and_encoded_ones_do_not():
    """THE DEFECT, PINNED. Unencoded 4-byte field names collide after the lossy decode; the
    same names encoded do not. Measured at 1 in 1,042 over 4,000,000 samples, so 20,000 samples
    is expected to produce roughly 190 colliding pairs -- far more than zero, which is what the
    encoded side must produce.
    """
    rng = random.Random(20261101)
    names = [rng.getrandbits(32).to_bytes(4, "little") for _ in range(20_000)]
    distinct = set(names)

    collapsed = {lossy(name) for name in distinct}
    encoded = {encode_field(name) for name in distinct}

    assert len(encoded) == len(distinct), (
        "the encoding must be injective: every distinct field name keeps its own identity"
    )
    assert len(collapsed) < len(distinct), (
        f"THE SERVER DEFECT IS GONE: all {len(distinct)} distinct field names survived the lossy "
        "decode, where this test expects roughly 1 in 1,042 pairs to collide. That means "
        "`string_arg` no longer decodes key and field arguments with String::from_utf8_lossy, "
        "which is the ONLY reason wire.py exists. This is the signal to DELETE wire.py, write "
        "Feast's bytes straight to the wire, and delete this test with it -- not to relax the "
        "assertion. Confirm against redis_dispatch.rs (HSET/HMGET/HKEYS/HDEL) before doing so, "
        "and re-materialize any rows written in the encoded shape."
    )


def test_a_glob_pattern_over_encoded_keys_matches_by_prefix_and_suffix():
    import fnmatch

    project = b"demo"
    prefix = b"\x00\x01driver_id"
    key = encode_key(prefix + b"\xff\x9c\x00payload" + project)
    pattern = encode_scan_pattern(prefix, project)
    assert fnmatch.fnmatchcase(key.decode("ascii"), pattern.decode("ascii"))

    other = encode_key(b"\x00\x01rider_id" + b"\xff" + project)
    assert not fnmatch.fnmatchcase(other.decode("ascii"), pattern.decode("ascii"))


def test_encoded_names_carry_no_glob_metacharacter():
    """An encoded name is embedded in a MATCH pattern, so it must not contain one."""
    raw = bytes(range(256))
    for encoded in (encode_key(raw), encode_field(raw)):
        text = encoded.decode("ascii")
        for meta in "*?[]\\":
            assert meta not in text, f"{meta!r} would be read as a pattern, not a literal"
