#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""The two ASGI fronts disagree about who may call, and that has to be on purpose.

`matrixark_asgi` and `matrixark_v1_gateway` are peer uvicorn entry points. Both build a
`MatrixArkMcpServer` from the environment, and both bind 0.0.0.0:8080 by default. They resolve
MATRIXARK_ACCESS_MODE to different things when an operator sets nothing:

    matrixark_asgi         -> "enforced"   an API key is required
    matrixark_v1_gateway   -> "dev"        anonymous, role dev_admin, every scope

In "dev" the access manager skips the scope check entirely and hands back an identity with
`sorted(MATRIXARK_ALL_SCOPES)`. That is a deliberate developer-experience default on the gateway
side, documented there and announced by a one-time startup warning. It is recorded here so that it
stays a decision: a change to either default, in either direction, fails this until someone updates
the table and says why.

The literals are read from the source rather than by importing, because importing either module
pulls in a server and its backends.
"""
from __future__ import annotations

import os
import re
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))

#: entry point -> (default access mode, why it is that)
DECLARED = {
    "matrixark_asgi.py": (
        "enforced",
        "a network front with no anonymous story of its own: it builds the server and serves it, "
        "so the safe end of the choice is the one that does not need a warning",
    ),
    "matrixark_v1_gateway.py": (
        "dev",
        "a deliberate developer-experience default so the API works with zero configuration, "
        "carried with require_auth=False, a DEV DEFAULT comment, and the one-time "
        "_NO_AUTH_WARNING logged at startup while the deployment is open",
    ),
}

_READ = re.compile(
    r'access_mode\s*=\s*os\.environ\.get\(\s*["\']MATRIXARK_ACCESS_MODE["\']\s*,\s*'
    r'["\']([a-z]+)["\']\s*\)')


def _default_in(name: str) -> str:
    with open(os.path.join(TOOLS, name), encoding="utf-8") as handle:
        found = _READ.findall(handle.read())
    return found[0] if len(found) == 1 else "|".join(found) or "NONE"


class TheTwoFrontsDisagreeOnPurpose(unittest.TestCase):

    def test_each_front_still_resolves_the_variable_once(self) -> None:
        """Assert this file's own extent: a moved read makes every check below vacuous."""
        for name in DECLARED:
            with self.subTest(entry_point=name):
                found = _default_in(name)
                self.assertIn(found, ("dev", "enforced"),
                              "%s no longer resolves MATRIXARK_ACCESS_MODE to a single literal "
                              "(found %r), so this file is asserting about something it can no "
                              "longer see" % (name, found))

    def test_the_defaults_are_the_declared_ones(self) -> None:
        actual = {name: _default_in(name) for name in DECLARED}
        expected = {name: value for name, (value, _) in DECLARED.items()}
        self.assertEqual(
            expected, actual,
            "an ASGI front changed which callers it admits by default. In 'dev' the access "
            "manager skips the scope check and returns role dev_admin with every scope, and both "
            "fronts bind 0.0.0.0 by default, so this is who can reach a deployment that "
            "configured nothing. Update the table above with the reason, or put the default back.")

    def test_every_declared_default_gives_a_reason(self) -> None:
        thin = sorted(name for name, (_, why) in DECLARED.items() if len(why.strip()) < 40)
        self.assertEqual([], thin,
                         "these are declared without a reason worth reading: %s" % thin)


if __name__ == "__main__":
    unittest.main()
