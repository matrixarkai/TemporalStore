# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Two agents under one user must not share a raw-blob prefix.

`_cloud_resource_prefix` decides where an ingested resource's raw bytes land. The account, tenant
and user components have always been there; `agent_id` was added for per-agent isolation and is
appended only when supplied, so that agent-less layouts stay byte-identical.

Nothing asserted any of that. The property was carried entirely by one comment beside the code, and
a second copy of the function in `matrixark_mcp_resources` had already lost it -- that copy built
`matrixark/raw/<acct>/<tenant>/<user>` for an agent-scoped envelope, one component short, so two
agents under the same user would have written to the same prefix. It was never on the live ingest
path (`matrixark_mcp_core`'s star-import delivers the `matrixark_mcp_core_resource_io` definition),
which is why it was latent rather than a leak, and it is now a re-export of the live one.

These tests are behavioural: they call the function and compare the paths it builds. A structural
check -- "does the source mention agent_id" -- would have passed on the copy that had dropped it,
because it still mentioned every other component.
"""

from __future__ import annotations

import pathlib
import sys
import unittest

TOOLS = pathlib.Path(__file__).resolve().parent
if str(TOOLS) not in sys.path:
    sys.path.insert(0, str(TOOLS))

SCOPE = {"account_id": "acct1", "tenant_id": "tenant1", "user_id": "user1"}


def _envelope(**scope: str) -> dict:
    return {"scope": dict(SCOPE, **scope), "metadata": {}}


class TwoAgentsDoNotShareARawBlobPrefixTest(unittest.TestCase):

    def setUp(self) -> None:
        try:
            import matrixark_mcp_core_resource_io as io_mod
        except ImportError as exc:  # pragma: no cover - the module is absent from this checkout
            self.skipTest("matrixark_mcp_core_resource_io is not importable here: %s" % exc)
        self.io_mod = io_mod
        self.prefix = io_mod._cloud_resource_prefix

    def test_the_function_builds_a_path_at_all(self) -> None:
        """Vacuity floor. Every comparison below is between two of its return values, so a
        function that returned the empty string for everything would satisfy the differences it is
        asked for and fail none of them."""
        built = self.prefix({}, _envelope())
        self.assertTrue(built, "the prefix builder returned nothing for a fully populated scope")
        for component in ("acct1", "tenant1", "user1"):
            self.assertIn(component, built,
                          "%r is missing from %r -- the builder is not reading the scope, and the "
                          "isolation tests below would compare two constants" % (component, built))

    def test_two_agents_under_one_user_get_different_prefixes(self) -> None:
        """The isolation property itself."""
        first = self.prefix({}, _envelope(agent_id="agent-alpha"))
        second = self.prefix({}, _envelope(agent_id="agent-beta"))
        self.assertNotEqual(
            first, second,
            "two agents under the same account/tenant/user build the same raw-blob prefix (%r). "
            "Their raw bytes share a location, and one agent's ingest can land under another's "
            "prefix." % first)

    def test_an_agent_scoped_prefix_extends_the_agent_less_one(self) -> None:
        """Isolation by EXTENSION, not by substitution: existing agent-less layouts keep their
        paths, which is what the comment beside the code promises."""
        bare = self.prefix({}, _envelope())
        scoped = self.prefix({}, _envelope(agent_id="agent-alpha"))
        self.assertTrue(
            scoped.startswith(bare + "/"),
            "an agent-scoped prefix (%r) no longer extends the agent-less one (%r). If the layout "
            "changed deliberately, existing raw blobs are no longer where their readers look."
            % (scoped, bare))

    def test_an_agent_less_envelope_is_unchanged_by_the_agent_component(self) -> None:
        """The other half of "appended only when supplied"."""
        self.assertEqual(
            self.prefix({}, _envelope()),
            self.prefix({}, {"scope": dict(SCOPE, agent_id=""), "metadata": {}}),
            "an empty agent_id now changes the prefix; agent-less layouts were meant to be "
            "byte-identical")

    def test_agent_and_session_both_appear_and_agent_comes_first(self) -> None:
        """Both components are appended, and their ORDER is the layout. Swapping them would keep
        every other assertion here true while moving every existing blob."""
        both = self.prefix({}, _envelope(agent_id="agent-alpha", session_id="sess-1"))
        agent_only = self.prefix({}, _envelope(agent_id="agent-alpha"))
        session_only = self.prefix({}, _envelope(session_id="sess-1"))
        self.assertTrue(both.startswith(agent_only + "/"),
                        "with both supplied, %r does not extend the agent-scoped prefix %r"
                        % (both, agent_only))
        self.assertNotEqual(both, session_only,
                           "a session-scoped prefix and an agent+session one are the same path")

    def test_the_second_module_re_exports_this_one_rather_than_copying_it(self) -> None:
        """`matrixark_mcp_resources` held a second definition that had drifted. It re-exports now;
        if a copy comes back, this says so before the paths diverge again."""
        try:
            import matrixark_mcp_resources as res_mod
        except ImportError as exc:  # pragma: no cover
            self.skipTest("matrixark_mcp_resources is not importable here: %s" % exc)
        self.assertIs(
            res_mod._cloud_resource_prefix, self.prefix,
            "matrixark_mcp_resources defines its own _cloud_resource_prefix again. The copy it "
            "had before never appended agent_id, so agent-scoped envelopes built a prefix one "
            "component short and two agents shared it.")


if __name__ == "__main__":
    unittest.main()
