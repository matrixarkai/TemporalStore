#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""A backend failure goes to the log, and the caller gets a token for it.

Fourteen handlers caught ``Exception`` and returned ``str(exc)`` to whoever made the call. That text
is whatever the backend raised -- an ``OSError`` naming a store directory, a connection error naming
an internal host and port, a driver message quoting the statement it choked on. Six of those sit on
routes reachable with a service key (``/v1/memories``, ``/v1/users``, ``/v1/skills``,
``/v1/skills/update``, ``/v1/memory/by-key``, ``/v1/ingest_file``), so the reader is often an
integrating application rather than an operator of the deployment.

It was also recorded nowhere: the gateway had a single logging call in five thousand lines, so the
exception text existed *only* in the response. Sanitising it on its own would have closed the leak by
destroying the only copy, which is why both halves are one change and why the checks below assert
both -- the text absent from the body **and** present in the log, tied together by a token.

What is deliberately unchanged: every handler catching a *typed* exception. ``UnknownSetting``,
``InvalidValue``, ``PathOutsideIngestionRoot``, ``JSONDecodeError`` and the rest carry messages
written to be read -- "must be one of x, y" is the entire answer to a rejected setting. A change
that made errors safer by making them useless would be the worse trade, so those are guarded here
too.
"""
from __future__ import annotations

import io
import json
import logging
import os
import re
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import matrixark_v1_gateway as gw  # noqa: E402

PORTAL = os.path.join(os.path.dirname(os.path.abspath(__file__)), "portal")

# Something a caller must never learn from an error: where this deployment keeps its pages.
SECRET = "/srv/matrixark/store/shard-7/pages.db"

# The routes whose reader is a service key, not an operator.
SERVICE_ROUTES = [
    ("POST", "/v1/memories"),
    ("POST", "/v1/users"),
    ("GET", "/v1/skills"),
]


def _drive(*args, **kwargs):
    from test_matrixark_v1_gateway import drive
    return drive(*args, **kwargs)


class _Boom:
    """A backend whose exception names something the caller must not learn."""

    def __init__(self, message=None):
        self.message = message or ("[Errno 13] Permission denied: '%s'" % SECRET)

    def call_tool(self, name, args):
        raise OSError(self.message)

    def handle(self, body):
        return {"jsonrpc": "2.0", "id": body.get("id"), "result": {}}


class _Capture:
    """Everything the gateway logger emits while this is held."""

    def __enter__(self):
        self.stream = io.StringIO()
        self.handler = logging.StreamHandler(self.stream)
        self.handler.setFormatter(logging.Formatter("%(levelname)s %(name)s %(message)s"))
        self.logger = logging.getLogger("matrixark.gateway")
        self.previous = self.logger.level
        self.logger.addHandler(self.handler)
        self.logger.setLevel(logging.ERROR)
        return self

    def __exit__(self, *_exc):
        self.logger.removeHandler(self.handler)
        self.logger.setLevel(self.previous)
        return False

    @property
    def text(self):
        return self.stream.getvalue()


def _call(method, path, server=None, body=None):
    from test_matrixark_v1_gateway import _cfg
    app = gw.make_v1_app(server or _Boom(), _cfg(require_auth=False))
    with _Capture() as log:
        status, _headers, payload = _drive(app, method=method, path=path,
                                           body=body if body is not None else {"scope": {}})
    return status, json.loads(payload), payload.decode("utf-8"), log.text


class TheCallerIsNotToldTheInsideTest(unittest.TestCase):

    def test_the_exception_text_does_not_reach_the_caller(self) -> None:
        for method, path in SERVICE_ROUTES:
            with self.subTest(route=path):
                _st, _body, raw, _log = _call(method, path)
                self.assertNotIn(SECRET, raw)
                self.assertNotIn("Errno 13", raw)

    def test_what_it_says_instead_is_about_the_call(self) -> None:
        _st, body, _raw, _log = _call("POST", "/v1/memories")
        self.assertEqual("backend_error", body["error"], "the machine-readable code is unchanged")
        self.assertIn("could not complete", body["detail"])

    def test_the_status_is_still_classified(self) -> None:
        """The code and status are how a client decides what to do; only the prose changed."""
        status, body, _raw, _log = _call("POST", "/v1/memories")
        self.assertGreaterEqual(status, 500)
        self.assertEqual("backend_error", body["error"])


class TheOperatorIsTest(unittest.TestCase):

    def test_the_exception_reaches_the_log(self) -> None:
        """The half that makes the other half honest. Without it this change would close the leak
        by throwing away the only copy of what went wrong."""
        _st, _body, _raw, log = _call("POST", "/v1/memories")
        self.assertIn(SECRET, log)

    def test_with_a_traceback(self) -> None:
        _st, _body, _raw, log = _call("POST", "/v1/memories")
        self.assertIn("Traceback", log)

    def test_the_log_names_the_call(self) -> None:
        _st, _body, _raw, log = _call("POST", "/v1/users")
        self.assertIn("POST", log)
        self.assertIn("/v1/users", log)


class TheTokenTiesThemTogetherTest(unittest.TestCase):

    def test_the_body_carries_one(self) -> None:
        _st, body, _raw, _log = _call("POST", "/v1/memories")
        self.assertTrue(body.get("incident"), body)

    def test_and_it_is_the_one_in_the_log(self) -> None:
        """A token that did not match would be worse than none: it would look like an answer."""
        _st, body, _raw, log = _call("POST", "/v1/memories")
        self.assertIn(body["incident"], log)

    def test_two_failures_are_told_apart(self) -> None:
        _st1, first, _r1, _l1 = _call("POST", "/v1/memories")
        _st2, second, _r2, _l2 = _call("POST", "/v1/memories")
        self.assertNotEqual(first["incident"], second["incident"])

    def test_it_is_short_enough_to_quote(self) -> None:
        _st, body, _raw, _log = _call("POST", "/v1/memories")
        self.assertRegex(body["incident"], r"^[0-9a-f]{8,16}$")


class ItIsVisibleWithoutBeingConfiguredTest(unittest.TestCase):
    """"Logged" must not mean "discarded". With no handler installed, Python writes WARNING and
    above to stderr through its last-resort handler, which is where a container's logs go."""

    def test_the_level_is_high_enough_to_survive_the_default(self) -> None:
        logger = logging.getLogger("matrixark.gateway")
        self.assertTrue(logger.isEnabledFor(logging.ERROR))

    def test_the_last_resort_handler_would_take_it(self) -> None:
        self.assertIsNotNone(logging.lastResort)
        self.assertLessEqual(logging.lastResort.level, logging.ERROR)

    def test_the_logger_is_named_so_a_deployment_can_redirect_it(self) -> None:
        with io.open(gw.__file__, encoding="utf-8") as handle:
            source = handle.read()
        self.assertIn('logging.getLogger("matrixark.gateway")', source)


class TheUsefulMessagesSurviveTest(unittest.TestCase):
    """A change that made errors safer by making them useless would be the worse trade."""

    def test_a_rejected_setting_still_says_what_is_wrong(self) -> None:
        from test_matrixark_v1_gateway import _cfg
        app = gw.make_v1_app(_Boom(), _cfg(require_auth=False))
        status, _h, payload = _drive(app, method="POST", path="/v1/admin/config",
                                     body={"settings": {"embedding.provider": "not-a-provider"}})
        body = json.loads(payload)
        self.assertEqual(400, status)
        self.assertEqual("invalid_value", body["error"])
        self.assertIn("embedding.provider", body["detail"],
                      "the reader needs to know which setting and why")

    def test_an_unknown_setting_still_names_it(self) -> None:
        from test_matrixark_v1_gateway import _cfg
        app = gw.make_v1_app(_Boom(), _cfg(require_auth=False))
        _st, _h, payload = _drive(app, method="POST", path="/v1/admin/config",
                                  body={"settings": {"no.such.setting": "1"}})
        body = json.loads(payload)
        self.assertEqual("unknown_setting", body["error"])
        self.assertIn("no.such.setting", body["detail"])

    def test_no_catch_all_handler_returns_the_exception_as_a_detail(self) -> None:
        """The three routes called above are a sample; this covers the other eleven sites.

        Worth having for a specific reason: the first mutation I ran against this file put the
        exception text back on one site, and the sample passed -- the site it landed on was not one
        of the three the sample calls. A sweep over the shapes is what makes reverting any of them
        fail.
        """
        with io.open(gw.__file__, encoding="utf-8") as handle:
            source = handle.read()
        leaks = re.findall(
            r'"error": "(backend_error|backend_unavailable|storage_quota_exceeded)",\s*'
            r'"detail": str\(exc\)', source)
        self.assertEqual([], leaks,
                         "these hand a caught-anything exception straight to the caller: %r" % leaks)

    def test_the_other_two_shapes_are_gone_too(self) -> None:
        """The same leak wearing different clothes: one assigns into a snapshot, one into an
        ingest response. Neither is a `detail` field, so the check above does not see them."""
        with io.open(gw.__file__, encoding="utf-8") as handle:
            source = handle.read()
        self.assertNotIn('{"status": "unavailable", "detail": str(exc)}', source)
        self.assertNotIn('out["extraction_error"] = str(exc)', source)

    def test_the_typed_handlers_were_left_alone(self) -> None:
        """Named so a later sweep cannot quietly generalise this change into them."""
        with io.open(gw.__file__, encoding="utf-8") as handle:
            source = handle.read()
        for code in ("invalid_value", "unknown_setting", "unknown_preset", "invalid_plan"):
            with self.subTest(code=code):
                self.assertRegex(source, r'"error": "%s", "detail": str\(exc\)' % code)


class ThePortalShowsTheTokenTest(unittest.TestCase):
    """A polite sentence with no token leaves the reader nowhere to go and the operator with a log
    they cannot tie to the report."""

    def _why(self, page_name="api_portal.html"):
        with io.open(os.path.join(PORTAL, page_name), encoding="utf-8") as handle:
            page = handle.read()
        at = page.index("window.__matrixarkWhy = function")
        end = page.index("\n  };", at) + 5
        return page[at:end]

    def test_every_page_appends_the_incident(self) -> None:
        missing = []
        for name in sorted(os.listdir(PORTAL)):
            if not name.endswith(".html"):
                continue
            with io.open(os.path.join(PORTAL, name), encoding="utf-8") as handle:
                if 'incident " + body.incident' not in handle.read():
                    missing.append(name)
        self.assertEqual([], missing, missing)

    def test_it_still_appends_the_scope_a_refusal_wanted(self) -> None:
        """The helper had one job before this and must keep it."""
        self.assertIn("this needs", self._why())


if __name__ == "__main__":
    unittest.main()
