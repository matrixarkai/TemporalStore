#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""`require_messages` is defined twice, and the copy in the module named `validation` is stricter.

Both modules are live. `matrixark_mcp_validation` is imported all over the tree -- `optional_object`
by 28 modules, `require_string` by 9 -- so it is not an orphan. But its `require_messages` is
SHADOWED: `matrixark_mcp_core_identity` defines its own, and that is the one
`matrixark_mcp_core.require_messages` binds and therefore the one the serving path runs.

The two disagree about what a valid message role is. MEASURED, by calling both:

    payload                             matrixark_mcp_core_identity (LIVE)   matrixark_mcp_validation
    role "user"                         ok                                   ok
    role "human"                        ok, rewritten to "user"              MatrixArkError
    role "ai"                           ok, rewritten to "assistant"         MatrixArkError
    role "USER"                         ok, rewritten to "user"              MatrixArkError

The live copy runs each role through `normalize_message_role`, accepts aliases, and records the
caller's spelling as `original_role`. The `validation` copy compares the raw string against a
four-item set, so it rejects every alias AND rejects "USER" purely on case.

They also differ in what they RETURN. The live copy returns newly built dicts with `role` rewritten
and `original_role` added; the `validation` copy returns the caller's own list object unchanged. So
even for input both accept, the two do not produce the same value.

WHY THIS IS WORTH RECORDING RATHER THAN FIXING. Anyone wiring up message validation would reach for
the module called `matrixark_mcp_validation`, and would silently get behaviour stricter than the
serving path -- uppercase roles refused, aliases refused. Making the two agree is an API decision:
widening `validation` changes what that module accepts, and narrowing the live copy would start
rejecting requests that work today. This records the split in both directions instead, so a new
divergence fails here and a resolved one fails here too.

This is the shadowing shape already recorded for the backend-policy functions, with the difference
that here the shadowed copy is the STRICTER one, so the shadowing is what keeps clients working.
"""
from __future__ import annotations

import os
import sys
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
if TOOLS not in sys.path:
    sys.path.insert(0, TOOLS)

import matrixark_mcp_core as core_module
import matrixark_mcp_core_identity as identity_module
import matrixark_mcp_validation as validation_module

#: role -> (does the live copy accept it, does the validation copy accept it)
RECORDED = {
    "user": (True, True),
    "human": (True, False),
    "ai": (True, False),
    "USER": (True, False),
}


def _accepts(module, role):
    """(accepted, resulting role) for one message with `role`."""
    try:
        out = module.require_messages({"messages": [{"role": role, "content": "hi"}]})
    except Exception:
        return False, None
    return True, out[0].get("role")


class TheMessageValidatorHasOneDefinition(unittest.TestCase):

    def test_both_copies_exist_and_agree_on_a_plain_role(self) -> None:
        """A floor. If either copy refuses everything, the table below says nothing."""
        for name, module in (("matrixark_mcp_core_identity", identity_module),
                             ("matrixark_mcp_validation", validation_module)):
            with self.subTest(module=name):
                accepted, role = _accepts(module, "user")
                self.assertTrue(accepted, "%s rejected a plain 'user' role" % name)
                self.assertEqual("user", role)

    def test_the_serving_path_binds_the_permissive_copy(self) -> None:
        """Which copy is live is the point; assert it rather than trusting the import."""
        bound = core_module.require_messages
        self.assertEqual(
            "matrixark_mcp_core_identity", bound.__module__.rsplit(".", 1)[-1],
            "matrixark_mcp_core.require_messages now comes from %s. If that is the validation "
            "module, requests with a role alias or an uppercase role start being refused."
            % bound.__module__)

    def test_the_two_copies_disagree_exactly_as_recorded(self) -> None:
        """Recorded, both directions, measured rather than read."""
        for role, (live_ok, strict_ok) in sorted(RECORDED.items()):
            with self.subTest(role=role):
                got_live, _ = _accepts(identity_module, role)
                got_strict, _ = _accepts(validation_module, role)
                self.assertEqual(
                    live_ok, got_live,
                    "matrixark_mcp_core_identity changed its answer for role %r" % role)
                self.assertEqual(
                    strict_ok, got_strict,
                    "matrixark_mcp_validation changed its answer for role %r. If the two now "
                    "agree, the split is resolved -- strike this file and say which won." % role)

    def test_an_alias_is_rewritten_and_the_original_kept(self) -> None:
        """What the live copy does that the other cannot: normalise and remember."""
        out = identity_module.require_messages(
            {"messages": [{"role": "human", "content": "hi"}]})
        self.assertEqual("user", out[0]["role"])
        self.assertEqual(
            "human", out[0].get("original_role"),
            "the live copy stopped recording the caller's own spelling as original_role")

    def test_the_strict_copy_returns_the_callers_list_unchanged(self) -> None:
        """The other half of the difference: one rewrites, the other passes through.

        Without this, 'they disagree about roles' would hide that they also disagree about the
        VALUE returned for input both accept.
        """
        payload = {"messages": [{"role": "user", "content": "hi"}]}
        returned = validation_module.require_messages(payload)
        self.assertIs(
            payload["messages"], returned,
            "matrixark_mcp_validation.require_messages now builds a new list; it used to return "
            "the caller's own object, which is half of what separates it from the live copy")
        live = identity_module.require_messages(
            {"messages": [{"role": "user", "content": "hi"}]})
        self.assertNotIn(
            "original_role", live[0],
            "a role needing no rewrite should not gain original_role")


if __name__ == "__main__":
    unittest.main()
