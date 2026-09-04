#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""A portal page is compressed if the client takes it, and not resent if the client has it.

Seven pages, 490 KB, served uncompressed with no validator on any of them -- there was no
``Content-Encoding``, no ``ETag`` and no ``Cache-Control`` anywhere in the gateway. Clicking
between panels re-downloaded the whole page every time, and 35% of what it re-downloaded was the
same shared stylesheet and script block the previous page already carried.

gzip takes the seven from 490 KB to 137 KB. A validator turns the second visit into a 304 with no
body at all.

The two details worth testing rather than assuming are the ones that fail quietly:

* ``Accept-Encoding: gzip;q=0`` is a *refusal*. Looking for the substring ``gzip`` reads it as
  consent and sends a body the client said it would not decode.
* the compressed and identity forms carry different validators, so a client holding one and
  revalidating while asking for the other gets fresh bytes rather than a 304 pointing at a copy in
  the wrong encoding.
"""
from __future__ import annotations

import gzip
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import matrixark_v1_gateway as gw  # noqa: E402

# Every route that serves a bundled page. A floor is asserted below so a renamed route cannot
# leave this sweep quantified over nothing.
PAGES = ["/v1/admin", "/v1/admin/portal", "/v1/admin/setup", "/v1/admin/catalog",
         "/v1/admin/explore", "/v1/admin/api", "/v1/admin/ingestion"]
GZIP = {"Accept-Encoding": "gzip, deflate"}


def _get(path, headers=None):
    """One request against a fresh app.

    The gateway suite's fixtures are imported HERE rather than at module level. Under
    `unittest discover` a test module is reachable as both `tools.X` and bare `X`, so importing one
    test module from another at import time pulls a second copy into the run and shifts what every
    later module sees -- which has previously shown up as CI failures in tests the branch never
    touched. See test_matrixark_no_cross_test_imports, whose recorded list may only shrink.
    """
    from test_matrixark_v1_gateway import _FakeServer, _cfg, drive
    app = gw.make_v1_app(_FakeServer(), _cfg())
    return drive(app, method="GET", path=path, headers=headers)


class TheHeaderIsParsedNotSearchedTest(unittest.TestCase):
    """`"gzip" in header` gets three of these wrong."""

    def test_a_plain_offer_is_accepted(self) -> None:
        self.assertTrue(gw._accepts_gzip("gzip"))
        self.assertTrue(gw._accepts_gzip("br, gzip"))
        self.assertTrue(gw._accepts_gzip("gzip;q=0.5"))

    def test_a_refusal_is_a_refusal(self) -> None:
        self.assertFalse(gw._accepts_gzip("gzip;q=0"))
        self.assertFalse(gw._accepts_gzip("deflate, gzip;q=0"))

    def test_a_wildcard_is_consent(self) -> None:
        self.assertTrue(gw._accepts_gzip("*"))
        self.assertTrue(gw._accepts_gzip("identity;q=0, *"))

    def test_nothing_asked_for_means_no(self) -> None:
        self.assertFalse(gw._accepts_gzip(""))
        self.assertFalse(gw._accepts_gzip("identity"))

    def test_junk_in_the_quality_does_not_raise(self) -> None:
        self.assertFalse(gw._accepts_gzip("gzip;q=banana"))


class TheSamePageIsSmallerOnTheWireTest(unittest.TestCase):

    def test_every_page_route_is_covered(self) -> None:
        self.assertGreaterEqual(len(PAGES), 7)
        for path in PAGES:
            with self.subTest(path=path):
                status, _h, _b = _get(path)
                self.assertEqual(200, status)

    def test_a_client_that_takes_gzip_gets_gzip(self) -> None:
        saved = 0
        for path in PAGES:
            with self.subTest(path=path):
                _st, plain_h, plain = _get(path)
                status, headers, packed = _get(path, GZIP)
                self.assertEqual(200, status)
                self.assertEqual("gzip", headers.get("content-encoding"))
                self.assertEqual(plain, gzip.decompress(packed),
                                 "the compressed body is not the page")
                self.assertLess(len(packed), len(plain))
                self.assertNotIn("content-encoding", plain_h)
                saved += len(plain) - len(packed)
        self.assertGreater(saved, 300000, "the whole point was the bytes")

    def test_the_length_describes_what_was_sent(self) -> None:
        """A content-length taken from the uncompressed body truncates or hangs the response."""
        for path in PAGES:
            with self.subTest(path=path):
                _st, headers, body = _get(path, GZIP)
                self.assertEqual(str(len(body)), headers.get("content-length"))

    def test_a_client_that_refuses_gets_the_page_itself(self) -> None:
        _st, headers, body = _get("/v1/admin/setup", {"Accept-Encoding": "gzip;q=0"})
        self.assertNotIn("content-encoding", headers)
        self.assertIn(b"<!doctype html", body[:200].lower())

    def test_a_cache_is_told_the_answer_depends_on_the_request(self) -> None:
        for headers in (None, GZIP):
            _st, got, _b = _get("/v1/admin/setup", headers)
            self.assertEqual("accept-encoding", got.get("vary"),
                             "a shared cache may hand a compressed body to a client that cannot "
                             "read one")


class TheSecondVisitCostsNothingTest(unittest.TestCase):

    def _etag(self, path, headers=None):
        _st, got, _b = _get(path, headers)
        return got.get("etag")

    def test_a_page_carries_a_validator(self) -> None:
        for path in PAGES:
            with self.subTest(path=path):
                self.assertTrue(self._etag(path), "nothing to revalidate against")

    def test_the_validator_is_the_same_next_time(self) -> None:
        """Derived from the bytes, not from the process -- otherwise a deployment behind several
        workers revalidates into a 200 whichever one answers."""
        self.assertEqual(self._etag("/v1/admin/setup"), self._etag("/v1/admin/setup"))

    def test_two_different_pages_do_not_share_one(self) -> None:
        self.assertNotEqual(self._etag("/v1/admin/setup"), self._etag("/v1/admin/catalog"))

    def test_holding_it_gets_a_304_with_no_body(self) -> None:
        for path in PAGES:
            with self.subTest(path=path):
                etag = self._etag(path, GZIP)
                headers = dict(GZIP)
                headers["If-None-Match"] = etag
                status, got, body = _get(path, headers)
                self.assertEqual(304, status)
                self.assertEqual(b"", body)
                self.assertEqual(etag, got.get("etag"))

    def test_holding_an_old_one_gets_the_page(self) -> None:
        headers = dict(GZIP)
        headers["If-None-Match"] = '"0000000000000000000000000000cafe"'
        status, _got, body = _get("/v1/admin/setup", headers)
        self.assertEqual(200, status)
        self.assertTrue(body)

    def test_a_wildcard_revalidation_is_honoured(self) -> None:
        status, _got, _b = _get("/v1/admin/setup", {"If-None-Match": "*"})
        self.assertEqual(304, status)


class TheEncodingsDoNotShareAValidatorTest(unittest.TestCase):
    """The failure this prevents is silent and total: a client reusing gzip bytes as HTML."""

    def test_the_two_forms_are_labelled_differently(self) -> None:
        _st, plain, _b = _get("/v1/admin/setup")
        _st2, packed, _b2 = _get("/v1/admin/setup", GZIP)
        self.assertNotEqual(plain.get("etag"), packed.get("etag"))

    def test_revalidating_across_encodings_gets_fresh_bytes(self) -> None:
        _st, packed, _b = _get("/v1/admin/setup", GZIP)
        status, _got, body = _get("/v1/admin/setup", {"If-None-Match": packed["etag"]})
        self.assertEqual(200, status, "a client would have reused gzip bytes as HTML")
        self.assertIn(b"<!doctype html", body[:200].lower())


class TheCompressionIsDoneOnceTest(unittest.TestCase):

    def test_the_same_page_packs_to_the_same_bytes(self) -> None:
        """gzip stamps the current time into its header by default, which would make every
        response different from the last and every validator a fresh one."""
        _st, _h, first = _get("/v1/admin/setup", GZIP)
        _st2, _h2, second = _get("/v1/admin/setup", GZIP)
        self.assertEqual(first, second)

    def test_the_cache_is_bounded(self) -> None:
        """Keyed by content, so a long-running process that reloads pages would otherwise keep one
        entry per version it ever served."""
        self.assertLessEqual(len(gw._HTML_PACKED), gw._HTML_PACKED_MAX)

    def test_a_short_body_is_left_alone(self) -> None:
        """An envelope on a few hundred bytes buys nothing and costs a decode."""
        self.assertLess(gw._HTML_PACK_FLOOR, 4096)


if __name__ == "__main__":
    unittest.main()
