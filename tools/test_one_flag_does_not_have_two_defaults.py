#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Two flags do not have *a* default. They have two, and the reader decides which applies.

A flag names one value. These two are read in different modules that fall back to different
things, so leaving the flag unset does not mean "the default" -- it means whichever default
belongs to the module that resolved it.

    MATRIXARK_EXTRACTION_API_KEY_ENV
        matrixark_mcp_core                 -> "ANTHROPIC_API_KEY"
        matrixark_mcp_extraction_provider  -> "OPENAI_API_KEY"

    MATRIXARK_EMBEDDING_TEXT_MAX_TOKENS
        matrixark_resource_parser          -> encoder_window_tokens(), which returns 512
        matrixark_v1_gateway               -> "128"

The first is the serious one, and it was confirmed by RUNNING it. That flag's whole job is to name
*which environment variable holds the extraction API key*. Unset, one reader looks for an Anthropic
key and the other for an OpenAI key, so the effective provider follows whichever module resolved
it rather than configuration:

    unset:  core='ANTHROPIC_API_KEY'  provider='OPENAI_API_KEY'   -> differ
    set:    core='MY_KEY'             provider='MY_KEY'           -> agree

The divergence exists ONLY on the default path, which is the path most deployments are on, and
setting the flag hides it. That is why nobody has tripped over it.

WHAT IS DELIBERATELY NOT HERE, because a first draft of this file got it wrong in both directions.

Asking `code_fallbacks()` for "more than one fallback value" reports ELEVEN flags. Nine of those
are not divergences:

  * Five -- TOP_K_PER_LAYER, MAX_CANDIDATES_PER_NODE, MAX_GLOBAL_CANDIDATES, MAX_SELECTED_REFS and
    CROSS_SESSION_PROFILE_MAX_CANDIDATES -- are the declared two-layer design. `READ_PATH_KNOBS`
    sets `layer = "read"` on exactly these, the tenant knob is the authoritative read-path value,
    and its help documents the choice ("Raised 24 -> 240"). `test_the_layering_is_still_declared`
    below ties the exclusion to that mechanism, so a knob dropping out of READ_PATH_KNOBS stops
    being excused and fails here.
  * EMBEDDING_API_BASE and EMBEDDING_API_KEY_ENV carry a voyage value and an openai value in the
    SAME function -- a provider branch, not a disagreement.
  * METADATA_DB reports 'root', '3306', '127.0.0.1' and 'matrixark': neighbouring reads of
    MATRIXARK_METADATA_USER, _PORT and _HOST that the scan grouped under one flag.
  * INGEST_TIMEOUT_MS and MAX_CONCURRENT_RETRIEVE carry three values each within one module.

So the rule asserted here is narrower than "more than one fallback": distinct values in DIFFERENT
modules, with the declared layering excused by name. The first draft recorded three flags by hand
and the exact-set assertion rejected it immediately -- which is the whole reason to assert a set
rather than a list of examples.
"""
from __future__ import annotations

import os
import sys
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
if TOOLS not in sys.path:
    sys.path.insert(0, TOOLS)

import matrixark_tenant_policy as tenant_policy
import test_the_flag_surface_only_shrinks as flag_surface

#: The one flag whose literal fallbacks differ across modules and is not declared layering.
RECORDED = {
    "MATRIXARK_EXTRACTION_API_KEY_ENV": {
        "matrixark_mcp_core.py": "ANTHROPIC_API_KEY",
        "matrixark_mcp_extraction_provider.py": "OPENAI_API_KEY",
    },
}

#: Recorded apart because one side is a CALL, not a literal, so the fallback scan cannot see it.
CALL_SIDED_MODULES = ("matrixark_resource_parser.py", "matrixark_v1_gateway.py")


def _layered_envs():
    """Env names of the knobs declared to be the read-path layer, excused by mechanism."""
    return {tenant_policy.KNOBS[name].env for name in tenant_policy.READ_PATH_KNOBS
            if name in tenant_policy.KNOBS and tenant_policy.KNOBS[name].env}


def _cross_module_divergences():
    """Flags whose literal fallback differs BETWEEN modules, layering excused."""
    layered = _layered_envs()
    out = {}
    for flag, values in flag_surface.code_fallbacks().items():
        by_module = {}
        for value, module in values:
            by_module.setdefault(module, set()).add(value)
        if len(by_module) < 2 or flag in layered:
            continue
        first = {sorted(v)[0] for v in by_module.values()}
        if len(first) > 1:
            out[flag] = {m: sorted(v)[0] for m, v in by_module.items()}
    return out


class OneFlagDoesNotHaveTwoDefaults(unittest.TestCase):

    def test_the_fallback_scan_is_actually_reading_something(self) -> None:
        """A floor. Every assertion below passes over an empty scan."""
        self.assertGreater(
            len(flag_surface.code_fallbacks()), 50,
            "the fallback scan sees almost nothing, so this file is comparing almost nothing")

    def test_the_layering_is_still_declared(self) -> None:
        """The exclusion is tied to its mechanism, not to a list kept here.

        Five flags are excused above because READ_PATH_KNOBS declares them the read-path layer. If
        one is renamed out of that tuple, it stops being excused and shows up as a divergence --
        which is the behaviour wanted, and this test says so out loud.
        """
        self.assertTrue(
            _layered_envs(),
            "READ_PATH_KNOBS resolved to no environment names, so the exclusion above is doing "
            "nothing and every layered pair would be reported as a divergence")

    def test_exactly_one_flag_diverges_across_modules(self) -> None:
        """Asserted in BOTH directions.

        A new flag whose unset value depends on the reader fails here. A resolved one fails here
        too, because somebody chose a default and the choice should be visible.
        """
        found = _cross_module_divergences()
        self.assertEqual(
            sorted(RECORDED), sorted(found),
            "the set of flags with a different fallback in different modules changed.\n"
            "  now:      %s\n  recorded: %s" % (sorted(found), sorted(RECORDED)))
        for flag, expected in RECORDED.items():
            with self.subTest(flag=flag):
                self.assertEqual(expected, found.get(flag))

    def test_the_extraction_key_flag_names_two_different_providers(self) -> None:
        """The consequence, stated as the thing that is actually wrong."""
        values = set(RECORDED["MATRIXARK_EXTRACTION_API_KEY_ENV"].values())
        self.assertEqual(
            {"ANTHROPIC_API_KEY", "OPENAI_API_KEY"}, values,
            "MATRIXARK_EXTRACTION_API_KEY_ENV no longer resolves to two different providers when "
            "unset. If one won, strike this file and record which.")

    def test_the_embedding_text_cap_still_differs_by_reader(self) -> None:
        """The call-sided pair, checked by CALLING it rather than reading the literal.

        `encoder_window_tokens()` returns 512 and the gateway falls back to 128, so the same unset
        flag caps embedding text at 4x different lengths depending on which module asks. A literal
        scan cannot see this, which is why it is asserted separately.
        """
        import matrixark_resource_parser as parser_module
        window = int(parser_module.encoder_window_tokens())
        self.assertNotEqual(
            128, window,
            "encoder_window_tokens() now returns 128, the same as the gateway's fallback, so the "
            "two readers agree and this entry should be struck")
        for module in CALL_SIDED_MODULES:
            with self.subTest(module=module):
                with open(os.path.join(TOOLS, module), encoding="utf-8",
                          errors="replace") as handle:
                    self.assertIn(
                        "MATRIXARK_EMBEDDING_TEXT_MAX_TOKENS", handle.read(),
                        "%s no longer reads MATRIXARK_EMBEDDING_TEXT_MAX_TOKENS; if the readers "
                        "were reconciled, strike this test" % module)


if __name__ == "__main__":
    unittest.main()
