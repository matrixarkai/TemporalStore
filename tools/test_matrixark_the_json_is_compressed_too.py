#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""The data the panels fetch is compressed, not just the pages that fetch it.

The portal's HTML was compressed and the JSON behind it was not. Measured over the app:

===================== ========= ========
route                 bytes     gzipped
===================== ========= ========
/v1/admin/config         78,610   17,338
/v1/admin/policy         19,406    6,304
/v1/admin/routes         15,197    4,131
/v1/admin/overview       13,194    4,410
===================== ========= ========

133,334 bytes across the admin surface, 35,058 compressed. The overview payload is re-read every
sixty seconds by a page somebody leaves open.

``_json`` is called from 197 places and none of them carry the request, so the ``Accept-Encoding``
is read from a context variable set once per request. asyncio copies the context per task, so one
request cannot see another's -- which is the thing worth testing about that choice, and it is tested
below by serving two requests that ask for different things.

Small bodies are left alone. gzip has an envelope: ``/v1/admin/api_key_usage`` is 41 bytes and
compresses to 57, and three of the eleven admin reads are in that range. Compressing everything
would make the smallest responses bigger and spend CPU doing it.
"""
from __future__ import annotations

import gzip
import json
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import matrixark_v1_gateway as gw  # noqa: E402

GZIP = {"Accept-Encoding": "gzip, deflate"}
BIG = "/v1/admin/config"
SMALL = "/v1/admin/api_key_usage"


def _drive(*args, **kwargs):
    """The gateway suite's driver, imported when called rather than at import time -- see
    test_matrixark_no_cross_test_imports."""
    from test_matrixark_v1_gateway import drive
    return drive(*args, **kwargs)


def _app():
    from test_matrixark_v1_gateway import _FakeServer, _cfg
    return gw.make_v1_app(_FakeServer(), _cfg(require_auth=False))


def _get(path, headers=None, app=None):
    return _drive(app or _app(), method="GET", path=path, headers=headers)


class TheBigReadsAreSmallerOnTheWireTest(unittest.TestCase):

    def test_a_large_payload_is_compressed(self) -> None:
        _st, headers, packed = _get(BIG, GZIP)
        self.assertEqual("gzip", headers.get("content-encoding"))
        _st2, _h2, plain = _get(BIG)
        self.assertLess(len(packed), len(plain))
        self.assertGreater(len(plain), 10000, "this route stopped being the big one")

    def test_it_is_the_same_json(self) -> None:
        """The failure that would be worst here: a body that decodes to something else."""
        _st, _h, packed = _get(BIG, GZIP)
        _st2, _h2, plain = _get(BIG)
        self.assertEqual(json.loads(plain), json.loads(gzip.decompress(packed)))

    def test_the_length_describes_what_was_sent(self) -> None:
        """A content-length taken from the uncompressed body truncates or hangs the response."""
        _st, headers, body = _get(BIG, GZIP)
        self.assertEqual(str(len(body)), headers.get("content-length"))

    def test_a_cache_is_told_the_answer_depends_on_the_request(self) -> None:
        for headers in (None, GZIP):
            _st, got, _b = _get(BIG, headers)
            self.assertEqual("accept-encoding", got.get("vary"))


class TheSmallOnesAreLeftAloneTest(unittest.TestCase):

    def test_a_short_payload_is_not_compressed(self) -> None:
        _st, headers, body = _get(SMALL, GZIP)
        self.assertNotIn("content-encoding", headers)
        self.assertLess(len(body), gw._JSON_PACK_FLOOR)

    def test_because_compressing_it_would_make_it_bigger(self) -> None:
        """The reason for the floor, asserted rather than asserted-about."""
        _st, _h, body = _get(SMALL)
        self.assertGreater(len(gzip.compress(body, 6, mtime=0)), len(body))


class OnlyWhenAskedTest(unittest.TestCase):

    def test_a_client_that_says_nothing_gets_json(self) -> None:
        _st, headers, body = _get(BIG)
        self.assertNotIn("content-encoding", headers)
        json.loads(body)

    def test_a_client_that_refuses_is_not_compressed(self) -> None:
        """`gzip;q=0` is a refusal; reading it as consent sends a body the client will not decode."""
        _st, headers, body = _get(BIG, {"Accept-Encoding": "gzip;q=0"})
        self.assertNotIn("content-encoding", headers)
        json.loads(body)


class OneRequestDoesNotSeeAnothersTest(unittest.TestCase):
    """The question a context variable raises, and the reason it is safe here."""

    def test_two_requests_on_one_app_are_negotiated_separately(self) -> None:
        app = _app()
        _st, packed_headers, packed = _get(BIG, GZIP, app)
        _st2, plain_headers, plain = _get(BIG, None, app)
        self.assertEqual("gzip", packed_headers.get("content-encoding"))
        self.assertNotIn("content-encoding", plain_headers)
        self.assertEqual(json.loads(plain), json.loads(gzip.decompress(packed)))

    def test_the_order_does_not_matter(self) -> None:
        app = _app()
        _st, plain_headers, _b = _get(BIG, None, app)
        _st2, packed_headers, _b2 = _get(BIG, GZIP, app)
        self.assertNotIn("content-encoding", plain_headers)
        self.assertEqual("gzip", packed_headers.get("content-encoding"))

    def test_the_default_compresses_nothing(self) -> None:
        """A helper called outside a request -- which a test may do -- has no encoding to honour."""
        self.assertEqual("", gw._ACCEPT_ENCODING.get())


class TheStreamIsNotTouchedTest(unittest.TestCase):
    """It builds its own headers and must keep doing so: a compressed event stream would be
    buffered rather than streamed, which is the one thing it cannot survive."""

    def test_the_event_route_is_not_json(self) -> None:
        source = gw.__file__
        with open(source, encoding="utf-8") as handle:
            text = handle.read()
        stream = text[text.index("text/event-stream"):]
        stream = stream[:stream.index("\n\n\n")] if "\n\n\n" in stream[:4000] else stream[:2000]
        self.assertNotIn("content-encoding", stream)


if __name__ == "__main__":
    unittest.main()
