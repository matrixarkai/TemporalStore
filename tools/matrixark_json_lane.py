# -*- coding: utf-8 -*-
"""The lane's JSON decoder, in one place.

A sampled profile of the running gateway put **53.5%** of its CPU in the stdlib JSON decoder, and
orjson parses the same text materially faster -- measured at -14.1% gateway CPU per message on an
identical corpus, with proxy CPU flat, which is the right control for a Python-side change.

That change was made in `matrixark_mcp_temporal_adapters`, and `matrixark_mcp_rust_proxy_client`
kept calling `json.loads` -- including for the whole context pack on every retrieve, which is the
largest payload on that path. Two modules serve the lane and the faster decoder reached one, so the
profile kept reporting `raw_decode` after the fix was supposedly in.

One behaviour differs and accepting it is deliberate: integers beyond u64 decode as floats rather
than exact ints. JSON guarantees no integer precision beyond 2**53 -- most parsers lose it far
earlier -- so a value that large is already outside what an interoperable consumer round-trips, and
every hash this system stores is within u64 and exact.

`orjson.JSONDecodeError` subclasses `json.JSONDecodeError`, so callers catching the stdlib error
catch this one unchanged. Optional by design: without orjson the stdlib parser is used and nothing
about the lane changes.
"""
from __future__ import annotations

try:  # pragma: no cover - whichever is installed is the one exercised
    import orjson as _orjson

    def lane_loads(text):
        """Parse lane JSON. Accepts str or bytes, as both parsers do."""
        return _orjson.loads(text)

    LANE_DECODER = "orjson"

except ImportError:  # pragma: no cover
    import json as _stdlib_json

    def lane_loads(text):
        """Parse lane JSON. Accepts str or bytes, as both parsers do."""
        if isinstance(text, (bytes, bytearray)):
            text = text.decode("utf-8")
        return _stdlib_json.loads(text)

    LANE_DECODER = "stdlib"
