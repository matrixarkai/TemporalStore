#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A limit the live path lets a tenant override, the extracted builder must let a tenant override.

matrixark_mcp_retrieve_planning is a partly-adopted extraction: LocalAdapter.retrieve imports it and
calls five of its helpers, but keeps its own inline copy of the ranking limits. The inline copy
resolves four of those limits through a per-tenant override. The extracted builder resolved none,
because it never took a scope -- so adopting it would have handed every tenant the build default
while looking like a pure code move.

That is the failure this checks for, and it is worth checking from source rather than from a list,
because the risk is not that somebody breaks the builder. It is that somebody adds a fifth
overridable limit to the live block and the extraction stays where it is.
"""
from __future__ import annotations

import ast
import os
import pathlib
import unittest
from unittest import mock

TOOLS = pathlib.Path(__file__).resolve().parent

LIVE_FILE = "matrixark_local_adapter_retrieve.py"
LIVE_HELPER = "_tenant_retrieval_limit"
EXTRACTED_FILE = "matrixark_mcp_retrieve_planning.py"
EXTRACTED_HELPER = "_tenant_ranking_limit"
BUILDER = "retrieval_ranking_limits"


def _names_passed_to(filename, helper):
    """The first (name) argument of every call to `helper` in `filename`."""
    tree = ast.parse((TOOLS / filename).read_text(encoding="utf-8"))
    found = set()
    for node in ast.walk(tree):
        if (isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
                and node.func.id == helper and node.args
                and isinstance(node.args[0], ast.Constant)
                and isinstance(node.args[0].value, str)):
            found.add(node.args[0].value)
    return found


def live_overridable():
    return _names_passed_to(LIVE_FILE, LIVE_HELPER)


def extracted_overridable():
    return _names_passed_to(EXTRACTED_FILE, EXTRACTED_HELPER)


def _builder_node():
    tree = ast.parse((TOOLS / EXTRACTED_FILE).read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == BUILDER:
            return node
    return None


class ATenantOverrideReachesTheExtractedRankingLimits(unittest.TestCase):

    def test_the_scan_finds_the_live_overrides(self):
        """The floor. With no live overrides found, every check below passes over nothing."""
        live = live_overridable()
        self.assertGreaterEqual(
            len(live), 4,
            "found %d limits resolved through %s in %s, expected at least 4 -- the scan stopped "
            "matching, so the comparison below proves nothing"
            % (len(live), LIVE_HELPER, LIVE_FILE))
        self.assertIsNotNone(_builder_node(), "%s is gone from %s" % (BUILDER, EXTRACTED_FILE))

    def test_every_limit_a_tenant_can_override_live_is_overridable_in_the_extraction(self):
        missing = sorted(live_overridable() - extracted_overridable())
        self.assertEqual(
            [], missing,
            "the live retrieve path lets a tenant override these, the extracted builder does not, "
            "so adopting it would hand every tenant the build default: %s" % ", ".join(missing))

    def test_the_extraction_does_not_invent_an_override_the_live_path_lacks(self):
        """Tighten in both directions: an override only the extraction applies is a divergence too,
        and it would change a limit on adoption just as silently."""
        extra = sorted(extracted_overridable() - live_overridable())
        self.assertEqual(
            [], extra,
            "the extracted builder applies a tenant override the live path does not: %s"
            % ", ".join(extra))

    def test_scope_is_required_so_a_caller_cannot_forget_it(self):
        """A defaulted scope is the defect wearing a signature: it would type-check, run, and
        quietly serve the build default to every tenant."""
        node = _builder_node()
        self.assertEqual(
            ["scope"], [a.arg for a in node.args.kwonlyargs],
            "%s must take scope as a keyword-only argument" % BUILDER)
        self.assertEqual(
            [None], node.args.kw_defaults,
            "scope has a default, so a caller can omit it and silently lose every tenant override")

    def test_an_override_actually_changes_the_limit(self):
        """Source says the call is there. This says the value arrives.

        The control is the same call with no override in place: if that also returned 7 the check
        above it would be reading its own stub.
        """
        try:
            from tools import matrixark_mcp_retrieve_planning as planning
            from tools import matrixark_tenant_policy as policy
        except ModuleNotFoundError:  # Direct script execution from tools/.
            import matrixark_mcp_retrieve_planning as planning
            import matrixark_tenant_policy as policy

        scope = {"tenant_id": "acme"}
        knob = policy.KNOBS.get("max_selected_refs")
        env_name = getattr(knob, "env", "") if knob is not None else ""
        environment = dict(os.environ)
        environment.pop(env_name, None)

        with mock.patch.dict(os.environ, environment, clear=True):
            with mock.patch.object(policy, "tenant_policy",
                                   return_value={"max_selected_refs": 7}):
                overridden = planning.retrieval_ranking_limits({}, scope=scope)
            with mock.patch.object(policy, "tenant_policy", return_value={}):
                plain = planning.retrieval_ranking_limits({}, scope=scope)

        self.assertEqual(7, overridden.max_selected_refs,
                         "the tenant override did not reach the extracted builder")
        self.assertNotEqual(
            7, plain.max_selected_refs,
            "the control returned the override's value with no override set, so the check above "
            "proves nothing")


if __name__ == "__main__":
    unittest.main()
