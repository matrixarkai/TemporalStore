#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""The portal sends the headers an admin interface is supposed to send.

It holds a bearer key in ``sessionStorage`` and drives operations that delete memory, revoke keys
and rewrite configuration. It was served with none of the headers that constrain what a page may do
or who may embed it -- no ``Content-Security-Policy``, no ``X-Frame-Options``, no
``X-Content-Type-Options``, no ``Referrer-Policy`` -- and its JSON reads carried no ``Cache-Control``
at all, so anything between the gateway and the browser was free to keep configuration, audit
records and per-key usage.

**Framing is the one that matters most.** Nothing stopped another site putting this portal in an
invisible frame over its own buttons, and Explore's forget, delete and reset are one click each
behind a confirm box -- as is revoking a key. Being framed is what turns a confirm box into a
decoration.

What makes a real policy possible here is how little the pages need: nothing is loaded
cross-origin, there are no inline event handlers, and there is no ``eval`` or ``new Function``
anywhere. Those three properties are asserted below, because the policy is only as tight as they
allow and a page that grew any of them would be broken by it -- silently, since a blocked script
does not announce itself.

``'unsafe-inline'`` is needed and is stated rather than implied: every page carries its stylesheet
and scripts inline, so this policy does **not** stop injected inline script. It stops the page being
framed, script being pulled from another origin, a ``<base>`` tag redirecting every relative URL,
and a form posting somewhere else.
"""
from __future__ import annotations

import io
import os
import re
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import matrixark_v1_gateway as gw  # noqa: E402

PORTAL = os.path.join(os.path.dirname(os.path.abspath(__file__)), "portal")
PAGES = ["/v1/admin", "/v1/admin/portal", "/v1/admin/setup", "/v1/admin/catalog",
         "/v1/admin/explore", "/v1/admin/api", "/v1/admin/ingestion"]


def _drive(*args, **kwargs):
    from test_matrixark_v1_gateway import drive
    return drive(*args, **kwargs)


def _get(path):
    from test_matrixark_v1_gateway import _FakeServer, _cfg
    app = gw.make_v1_app(_FakeServer(), _cfg(require_auth=False))
    return _drive(app, method="GET", path=path)


def pages() -> dict:
    return {name: io.open(os.path.join(PORTAL, name), encoding="utf-8").read()
            for name in sorted(os.listdir(PORTAL)) if name.endswith(".html")}


class EveryPageSaysWhoMayEmbedItTest(unittest.TestCase):

    def test_every_page_route_answers(self) -> None:
        self.assertGreaterEqual(len(PAGES), 7)
        for path in PAGES:
            with self.subTest(path=path):
                status, _h, _b = _get(path)
                self.assertEqual(200, status)

    def test_nobody_may_frame_it(self) -> None:
        """Explore's forget, delete and reset are one click each behind a confirm box, and so is
        revoking a key. A frame over someone else's buttons is what makes that click theirs."""
        for path in PAGES:
            with self.subTest(path=path):
                _st, headers, _b = _get(path)
                self.assertIn("frame-ancestors 'none'", headers.get("content-security-policy", ""))
                self.assertEqual("DENY", headers.get("x-frame-options"),
                                 "older intermediaries honour this one and not the other")

    def test_a_response_cannot_be_sniffed_into_something_else(self) -> None:
        for path in PAGES:
            with self.subTest(path=path):
                _st, headers, _b = _get(path)
                self.assertEqual("nosniff", headers.get("x-content-type-options"))

    def test_the_admin_url_does_not_travel(self) -> None:
        _st, headers, _b = _get("/v1/admin/setup")
        self.assertEqual("no-referrer", headers.get("referrer-policy"))

    def test_the_policy_does_not_allow_eval(self) -> None:
        """Nothing in the portal needs it, and allowing it would give back most of what the rest
        of the policy buys."""
        _st, headers, _b = _get("/v1/admin/setup")
        self.assertNotIn("unsafe-eval", headers.get("content-security-policy", ""))

    def test_script_cannot_come_from_another_origin(self) -> None:
        _st, headers, _b = _get("/v1/admin/setup")
        policy = headers.get("content-security-policy", "")
        self.assertIn("default-src 'self'", policy)
        self.assertIn("base-uri 'none'", policy)
        self.assertIn("form-action 'none'", policy)


class TheAdminDataIsNotKeptTest(unittest.TestCase):

    def test_json_is_not_stored_anywhere(self) -> None:
        """Configuration names the endpoints this deployment calls; the audit route returns who
        reached for what. Neither should sit in an intermediary."""
        for path in ("/v1/admin/config", "/v1/admin/overview", "/v1/admin/audit"):
            with self.subTest(path=path):
                _st, headers, _b = _get(path)
                self.assertEqual("no-store", headers.get("cache-control"))

    def test_json_cannot_be_sniffed_either(self) -> None:
        _st, headers, _b = _get("/v1/admin/config")
        self.assertEqual("nosniff", headers.get("x-content-type-options"))
        self.assertEqual("no-referrer", headers.get("referrer-policy"))


class ThePagesStayInsideThePolicyTest(unittest.TestCase):
    """The policy is exactly as tight as the pages allow. A page that grew any of these would be
    broken by it, and a blocked script does not announce itself."""

    def test_nothing_is_loaded_cross_origin(self) -> None:
        external = {}
        for name, text in pages().items():
            found = re.findall(r'(?:src|href)="(?:https?:)?//[^"]+"', text)
            if found:
                external[name] = found[:3]
        self.assertEqual({}, external, "these would be blocked by default-src 'self': %r" % external)

    def test_there_are_no_inline_event_handlers(self) -> None:
        """`onclick=` and friends are blocked by a policy without 'unsafe-hashes'. The portal wires
        events in script instead, and this keeps it that way."""
        offenders = {}
        for name, text in pages().items():
            found = re.findall(r"\son(?:click|change|input|submit|load|error)=", text)
            if found:
                offenders[name] = len(found)
        self.assertEqual({}, offenders, offenders)

    def test_nothing_evaluates_a_string(self) -> None:
        offenders = sorted(name for name, text in pages().items()
                           if re.search(r"\beval\(|new Function\(", text))
        self.assertEqual([], offenders,
                         "these would need 'unsafe-eval', which the policy does not grant: %r"
                         % offenders)

    def test_the_sweep_sees_the_pages(self) -> None:
        self.assertGreaterEqual(len(pages()), 7)


if __name__ == "__main__":
    unittest.main()
