#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""The published API surface must be the one this process serves.

`ROUTE_DOCS` is what a customer reads on `/v1/admin/api` and gets from `GET /v1/admin/routes`. A
route that exists and is not in it is one they cannot find; a route in it that no longer exists is
worse, because they will write against it and only find out in production. So the list is compared
against the path literals in the gateway itself, in both directions, rather than maintained by
hand and hoped over.

Serving is discovered the way the dispatcher does it: the string constants compared against `path`,
the `path.startswith(...)` prefixes, and the `_DATA_ROUTES` table.
"""
from __future__ import annotations

import ast
import io
import json
import os
import sys
import unittest
from typing import Set

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import matrixark_v1_gateway as gw  # noqa: E402
from test_matrixark_v1_gateway import _FakeServer, _cfg, drive  # noqa: E402

SOURCE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "matrixark_v1_gateway.py")

# `/v1` and `/v1/*` are the prefix guard that decides whether the gateway handles a request at all,
# not routes; `/v1/admin/` is the trailing-slash alias of `/v1/admin`; the `/cancel` suffix belongs
# to the job-cancel route, which is documented under its full shape.
NOT_ROUTES = {"/v1", "/v1/*", "/v1/admin/", "/cancel*", "/retry*"}

# A served prefix, and the documented shapes that stand for it. A prefix can carry more than one
# action -- /v1/admin/ingestion/jobs/{id}/ serves both cancel and retry -- so this maps to a set.
PREFIX_SHAPES = {
    "/v1/memory/*": {"/v1/memory/{id}", "/v1/memory/{id}/history"},
    "/v1/blob/*": {"/v1/blob/{key}"},
    "/v1/admin/ingestion/jobs/*": {"/v1/admin/ingestion/jobs/{id}/cancel",
                                   "/v1/admin/ingestion/jobs/{id}/retry"},
    "/v1/admin/monitoring/*": {"/v1/admin/monitoring/{asset}"},
}


def _served_paths() -> Set[str]:
    tree = ast.parse(io.open(SOURCE, encoding="utf-8").read())
    found: Set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Compare) and isinstance(node.left, ast.Name) \
                and node.left.id == "path":
            for comparator in node.comparators:
                if isinstance(comparator, ast.Constant) and isinstance(comparator.value, str):
                    found.add(comparator.value)
                elif isinstance(comparator, (ast.Tuple, ast.List)):
                    for element in comparator.elts:
                        if isinstance(element, ast.Constant) and isinstance(element.value, str):
                            found.add(element.value)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) \
                and node.func.attr == "startswith" and isinstance(node.func.value, ast.Name) \
                and node.func.value.id == "path":
            for arg in node.args:
                if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
                    found.add(arg.value + "*")
    found |= set(gw._DATA_ROUTES)
    resolved = set()
    for path in found:
        if path in NOT_ROUTES:
            continue
        resolved |= PREFIX_SHAPES.get(path, {path})
    return resolved


SERVED = _served_paths()
DOCUMENTED = {entry["path"] for entry in gw.ROUTE_DOCS}


class RouteDocsTest(unittest.TestCase):
    def test_every_served_route_is_documented(self) -> None:
        self.assertEqual(set(), SERVED - DOCUMENTED,
                         "routes this gateway serves that /v1/admin/api never mentions")

    def test_nothing_documented_has_stopped_existing(self) -> None:
        self.assertEqual(set(), DOCUMENTED - SERVED,
                         "routes the published list promises that are no longer served")

    def test_every_entry_is_usable_as_written(self) -> None:
        groups = set()
        for entry in gw.ROUTE_DOCS:
            with self.subTest(route="%s %s" % (entry["method"], entry["path"])):
                self.assertIn(entry["method"], ("GET", "POST", "PUT", "DELETE"))
                self.assertTrue(entry["summary"].strip())
                self.assertTrue(entry["group"].strip())
                groups.add(entry["group"])
                if entry["method"] == "POST" and not entry.get("raw_body") \
                        and not entry.get("stream") and "{id}" not in entry["path"]:
                    # A POST a customer is told about should come with a body that works, not a
                    # schema to translate.
                    self.assertIn("body", entry,
                                  "%s has no example body" % entry["path"])
                if "body" in entry:
                    json.dumps(entry["body"])  # must serialise
        self.assertIn("Memory", groups)
        self.assertIn("Administration", groups)

    def test_the_scope_named_is_one_the_backend_knows(self) -> None:
        from matrixark_mcp_core import MATRIXARK_TOOL_SCOPES
        enforced = {s for scopes in MATRIXARK_TOOL_SCOPES.values() for s in scopes}
        for entry in gw.ROUTE_DOCS:
            scope = entry.get("scope")
            if not scope or scope in ("admin", "per tool"):
                continue
            with self.subTest(route=entry["path"]):
                self.assertIn(scope, enforced)

    def test_the_list_is_served_without_credentials(self) -> None:
        # It is the published contract; every route it names enforces its own access.
        app = gw.make_v1_app(_FakeServer(), _cfg())
        status, headers, body = drive(app, method="GET", path="/v1/admin/routes")
        self.assertEqual(200, status)
        self.assertTrue(headers["content-type"].startswith("application/json"))
        self.assertEqual(len(gw.ROUTE_DOCS), len(json.loads(body)["routes"]))

    def test_every_documented_example_actually_works(self) -> None:
        """Run each example the API page offers and require a 2xx.

        A published example that 400s is worse than no example: it is copied first and read second,
        and the reader concludes the route is broken rather than the sample. Three shapes cannot be
        exercised here and are excluded for stated reasons: a raw-body upload needs a blob tier, a
        path with an {id} needs one that exists, and a STREAM has no response to wait for -- it
        stays open by design, so driving it here simply hangs, which is exactly what happened the
        first time one was documented. Streams are covered by their own test, which reads a frame
        and disconnects. A route that says it `needs` a setting this deployment has not got is
        allowed to refuse.
        """
        server = _FakeServer()
        app = gw.make_v1_app(server, _cfg())
        headers = {"Authorization": "Bearer k-acme"}
        checked = 0
        for entry in gw.ROUTE_DOCS:
            if entry.get("raw_body") or entry.get("stream") or "{" in entry["path"]:
                continue
            path = entry["path"]
            if entry.get("query"):
                path += "?" + entry["query"]
            with self.subTest(route="%s %s" % (entry["method"], entry["path"])):
                status, _hdrs, body = drive(app, method=entry["method"], path=path,
                                            headers=headers, body=entry.get("body"))
                if entry.get("needs") and status == 400:
                    continue  # refused for want of the setting it names, which is correct
                checked += 1
                self.assertLess(status, 300,
                                "%s %s answered %d: %s"
                                % (entry["method"], path, status, body[:200]))
        self.assertGreater(checked, 25, "the sweep did not actually exercise the surface")

    def test_a_streaming_route_says_so(self) -> None:
        # The flag is what keeps the sweep above from hanging on it, so it is worth asserting
        # rather than leaving as a convention someone can forget.
        streaming = [entry for entry in gw.ROUTE_DOCS if entry.get("stream")]
        self.assertTrue(streaming, "no route is marked as a stream")
        for entry in streaming:
            with self.subTest(route=entry["path"]):
                self.assertNotIn("body", entry)
                self.assertIn("stream", entry["summary"].lower())

    def test_a_get_example_that_needs_a_query_parameter_carries_one(self) -> None:
        # /v1/memory/by-key without identity_key is a 400 by design; an example missing it teaches
        # the wrong thing about the route.
        by_key = [e for e in gw.ROUTE_DOCS if e["path"] == "/v1/memory/by-key"][0]
        self.assertIn("identity_key", by_key.get("query", ""))

    def test_the_api_page_is_served(self) -> None:
        app = gw.make_v1_app(_FakeServer(), _cfg())
        status, headers, body = drive(app, method="GET", path="/v1/admin/api")
        self.assertEqual(200, status)
        self.assertTrue(headers["content-type"].startswith("text/html"))
        self.assertIn(b"portalnav", body)


if __name__ == "__main__":
    unittest.main()
