#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Every admin operation is fenced to the caller's own tenant.

Two holes of this shape were found and fixed on the same day, in two different layers:

  * `revoke_api_key` never called `ensure_identity_can_manage`, though `create_api_key` did. An
    admin key for one tenant could revoke another tenant's key. `rotate_api_key` inherited it and
    made it worse: it revoked before it authorized, so a *refused* rotation destroyed the key it
    had just been refused permission to touch.
  * `GET /v1/admin/api_key_usage` returned the meter's snapshot whole. That snapshot is
    deployment-wide -- every metered key's hash, tenant, account, request counts and bytes -- so
    one tenant's admin read every other tenant's traffic volumes.

Neither was a missing guard. Both guards existed and were called from the neighbouring operations;
what was missing was the call. That is not something you find by grepping for the guard's
definition -- it is there, and it proves nothing. You find it by listing the operations and the
call sites and diffing the two, which is what this does.

The checks are pure functions over source text so they can be run against deliberately broken
copies. Each one is paired with a case that feeds it a hole and requires it to notice: a checker
that only ever sees healthy input passes just as well when it has stopped checking.
"""
from __future__ import annotations

import io
import os
import re
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))

GUARD = "ensure_identity_can_manage"
GATEWAY = os.path.join(TOOLS, "matrixark_v1_gateway.py")
CORE = os.path.join(TOOLS, "matrixark_mcp_core.py")
ACCESS_MODULES = ("matrixark_access_apikey", "matrixark_access_accounts", "matrixark_access_sso",
                  "matrixark_access_portal", "matrixark_access")

# Floors, so a pattern that stopped matching cannot leave these checks quantified over nothing.
MIN_ADMIN_TOOLS = 13
MIN_TOOL_ROUTES = 3

# `list_accounts` is fenced by construction instead of by the guard: outside dev mode it discards
# the requested account and substitutes the caller's own, so it cannot reach another one. Named
# here with its reason, and the reason is asserted below rather than taken on trust.
FENCED_BY_CONSTRUCTION = {"list_accounts"}


def read(path: str) -> str:
    with io.open(path, encoding="utf-8") as handle:
        return handle.read()


def admin_tools(core_source: str) -> list:
    return sorted(set(re.findall(r'"(matrixark_admin_\w+)":\s*\{', core_source)))


def method_bodies(sources: dict) -> dict:
    bodies = {}
    for source in sources.values():
        for match in re.finditer(r"\n    def (\w+)\(self.*?(?=\n    def |\Z)", source, re.S):
            bodies.setdefault(match.group(1), match.group(0))
    return bodies


def unfenced_methods(core_source: str, sources: dict) -> list:
    """Admin operations that neither call the guard nor force the caller's own account."""
    bodies = method_bodies(sources)
    unfenced = []
    for tool in admin_tools(core_source):
        name = tool[len("matrixark_admin_"):]
        body = bodies.get(name)
        if body is None:
            unfenced.append(name + " (method not found)")
            continue
        if GUARD in body:
            continue
        if name in FENCED_BY_CONSTRUCTION:
            continue
        unfenced.append(name)
    return unfenced


def admin_route_blocks(gateway_source: str) -> list:
    """(route, block) for each `/v1/admin/...` branch, up to the next one."""
    lines = gateway_source.split("\n")
    starts = [i for i, line in enumerate(lines)
              if "if method ==" in line and re.search(r'path == "/v1/admin/[^"]*"', line)]
    blocks = []
    for index, start in enumerate(starts):
        end = starts[index + 1] if index + 1 < len(starts) else len(lines)
        route = re.search(r'path == "([^"]+)"', lines[start]).group(1)
        blocks.append((route, "\n".join(lines[start:end])))
    return blocks


def unscoped_routes(gateway_source: str) -> list:
    """Admin routes that call a backend tool without narrowing it to the caller."""
    unscoped = []
    for route, block in admin_route_blocks(gateway_source):
        if "server.call_tool" not in block:
            continue
        if "_apply_identity" not in block:
            unscoped.append(route)
    return unscoped


class EveryAdminOperationChecksTheCallerTest(unittest.TestCase):

    def setUp(self) -> None:
        self.core = read(CORE)
        self.sources = {}
        for name in ACCESS_MODULES:
            path = os.path.join(TOOLS, name + ".py")
            if os.path.exists(path):
                self.sources[name] = read(path)

    def test_the_sweep_covers_the_operations_it_claims_to(self) -> None:
        found = admin_tools(self.core)
        self.assertGreaterEqual(len(found), MIN_ADMIN_TOOLS,
                                "found %d admin tools, expected at least %d: %r"
                                % (len(found), MIN_ADMIN_TOOLS, found))

    def test_no_admin_operation_skips_the_guard(self) -> None:
        self.assertEqual([], unfenced_methods(self.core, self.sources),
                         "these admin operations neither call %s nor force the caller's own "
                         "account, so they can act on another tenant" % GUARD)

    def test_the_one_exception_really_is_fenced_by_construction(self) -> None:
        """Named in a list, so the reason has to hold rather than be remembered."""
        body = method_bodies(self.sources)["list_accounts"]
        self.assertIn('identity.get("mode") != "dev"', body)
        self.assertIn('requested_account = identity["account_id"]', body,
                      "list_accounts no longer substitutes the caller's own account, so being "
                      "exempt from the guard leaves it open")

    def test_a_missing_guard_would_be_caught(self) -> None:
        """The positive control. Take the guard out of one operation and require a complaint."""
        doctored = dict(self.sources)
        # The call has to go entirely: leaving the name behind in a comment satisfies the check
        # and makes the control pass while proving nothing. (It did, the first time.)
        call = ("self." + GUARD
                + '(identity, record["account_id"], record["tenant_id"])')
        doctored["matrixark_access_apikey"] = doctored["matrixark_access_apikey"].replace(
            call, "pass", 1)
        self.assertNotEqual(self.sources["matrixark_access_apikey"],
                            doctored["matrixark_access_apikey"],
                            "the doctoring matched nothing, so this proves nothing")
        self.assertIn("revoke_api_key", unfenced_methods(self.core, doctored),
                      "a revocation with no authorization check went unnoticed")


class EveryAdminRouteNarrowsWhatItAsksForTest(unittest.TestCase):

    def setUp(self) -> None:
        self.gateway = read(GATEWAY)

    def test_the_sweep_covers_the_routes_it_claims_to(self) -> None:
        with_tools = [route for route, block in admin_route_blocks(self.gateway)
                      if "server.call_tool" in block]
        self.assertGreaterEqual(len(with_tools), MIN_TOOL_ROUTES,
                                "found %d admin routes calling a backend tool, expected at least "
                                "%d: %r" % (len(with_tools), MIN_TOOL_ROUTES, with_tools))

    def test_no_admin_route_asks_the_backend_for_more_than_the_caller_owns(self) -> None:
        self.assertEqual([], unscoped_routes(self.gateway),
                         "these routes call a backend tool without narrowing it to the caller")

    def test_an_unscoped_route_would_be_caught(self) -> None:
        # One whole route loses its narrowing, rather than the first occurrence in the file --
        # that one sits outside any tool-calling branch, so removing it changed nothing and the
        # control passed while testing nothing.
        target = [(route, block) for route, block in admin_route_blocks(self.gateway)
                  if "server.call_tool" in block and "_apply_identity" in block]
        self.assertTrue(target, "no route to doctor")
        route, block = target[0]
        doctored = self.gateway.replace(block, block.replace("_apply_identity", "pass  #"), 1)
        self.assertNotEqual(self.gateway, doctored, "the doctoring matched nothing")
        self.assertIn(route, unscoped_routes(doctored),
                      "a route that asks the backend for everything went unnoticed")

    def test_the_usage_snapshot_is_narrowed_before_it_is_returned(self) -> None:
        """It is deployment-wide, and the route returns it directly rather than through a tool, so
        the check above cannot see it."""
        block = [b for route, b in admin_route_blocks(self.gateway)
                 if route == "/v1/admin/api_key_usage"]
        self.assertEqual(1, len(block), "the usage route was not found")
        self.assertIn("_usage_rows_visible_to", block[0],
                      "the usage read returns every tenant's rows again")


if __name__ == "__main__":
    unittest.main()
