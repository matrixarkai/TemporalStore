#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""The extracted ContextPack cache key must separate everything the live one separates.

LocalAdapter.retrieve builds its ContextPack cache key inline. The function
matrixark_mcp_retrieve_cache.context_pack_cache_key is the extracted form of that tuple, and it
exists so the inline code can be replaced by it -- which is exactly why it is worth checking now,
while nothing production depends on it.

A cache key that drops a discriminator does not fail; it serves the wrong pack. The inline tuple
says so itself, beside the return-all entry: a tenant that turns return-all on and asks the same
question again must not be handed the ranking built before it. That was measured, not imagined, and
the same sentence covers the cross-session and shared-context policies.

This is derived from the two sources rather than from a written list, because a list cannot see the
discriminator somebody adds to the live key next month -- which is how the extracted copy fell
behind in the first place.

Matching is by ATOM, not by text. The extracted builder takes a ranking dict, and its caller folds
several discriminators into that dict before passing it, so a purely textual diff of the two tuples
reports six divergences that are not real. The fold is read here as part of the extracted key,
because that is what it is.
"""
from __future__ import annotations

import ast
import pathlib
import unittest

TOOLS = pathlib.Path(__file__).resolve().parent

LIVE_FILE = "matrixark_local_adapter_retrieve.py"
LIVE_KEY = "pack_cache_key"
EXTRACTED_FILE = "matrixark_mcp_retrieve_cache.py"
EXTRACTED_KEY = "context_pack_cache_key"
CALLER_FILE = "matrixark_mcp_retrieve_request.py"
CALLER_FOLD = "cache_ranking"

#: Atoms that carry no discriminating meaning -- they appear in nearly every element of both keys,
#: so treating them as coverage would let any element match anything.
PLUMBING = frozenset({
    "args", "get", "json", "dumps", "sort_keys", "separators", "bool", "int", "str", "float",
    "tuple", "sorted", "set", "len", "self", "target", "enabled", "status", "True", "False",
    # The two halves of separators=(",", ":"). They are string CONSTANTS, so they read as atoms,
    # and nearly every element of both keys is a json.dumps that carries them -- which made every
    # dumped element look covered by every other one. cross_session_policy read as covered by a
    # comma. test_an_element_is_never_covered_by_punctuation_alone is here because of it.
    ",", ":",
})

#: A live discriminator the extracted key does not carry, and why. Every entry has to still be
#: diverging -- see test_no_recorded_divergence_has_quietly_been_fixed -- so this list can only be
#: shortened by fixing something, never lengthened to go green.
RECORDED_DIVERGENCE = {
    "debug_refs":
        "applied on READ, not on store: get_cached_context_pack compacts with include_debug at "
        "return time, so one cached pack serves both answers. The live tuple keys on it because "
        "the live reader returns the UNCOMPACTED pack for debug_context_pack, and the extracted "
        "reader has no such branch.",
    "debug_context_pack":
        "same branch, and it carries include_retrieval_debug with it -- the live tuple reads the "
        "two in one expression. The live reader can return the raw pack, the extracted reader "
        "always compacts, so neither flag changes what the extracted reader hands back.",
    "retrieval_return_all":
        "NOT covered, and it should be. It comes from _return_all_candidates(scope) in "
        "matrixark_local_adapter_retrieve, a private wrapper over matrixark_index_growth_bound. "
        "The caller cannot fold what it cannot call, and copying the wrapper would duplicate its "
        "fallback rather than share it. Adopting the extraction needs that wrapper to have a "
        "shared home FIRST. Keying on the scope does not cover it: the policy can be turned on "
        "for a scope that has not otherwise changed, which is the measured incident.",
    "retrieval_return_all_threshold":
        "the other half of the same wrapper, with the same answer.",
}


def _atoms(node):
    """The names and literal strings inside one key element, minus the plumbing."""
    found = set()
    for sub in ast.walk(node):
        if isinstance(sub, ast.Name):
            found.add(sub.id)
        elif isinstance(sub, ast.Attribute):
            found.add(sub.attr)
        elif isinstance(sub, ast.Constant) and isinstance(sub.value, str):
            found.add(sub.value)
    return {a.lstrip("_") for a in found if a not in PLUMBING} - PLUMBING


def _parse(name):
    return ast.parse((TOOLS / name).read_text(encoding="utf-8"))


def live_elements():
    for node in ast.walk(_parse(LIVE_FILE)):
        if (isinstance(node, ast.Assign)
                and any(isinstance(t, ast.Name) and t.id == LIVE_KEY for t in node.targets)
                and isinstance(node.value, ast.Tuple)):
            return list(node.value.elts)
    return []


def extracted_elements():
    for node in ast.walk(_parse(EXTRACTED_FILE)):
        if isinstance(node, ast.FunctionDef) and node.name == EXTRACTED_KEY:
            for sub in ast.walk(node):
                if isinstance(sub, ast.Return) and isinstance(sub.value, ast.Tuple):
                    return list(sub.value.elts)
    return []


def folded_into_ranking():
    """What the caller merges into the ranking dict before handing it to the extracted builder.

    The builder json.dumps the whole dict, so anything folded in here is in the key.
    """
    for node in ast.walk(_parse(CALLER_FILE)):
        if (isinstance(node, ast.Assign)
                and any(isinstance(t, ast.Name) and t.id == CALLER_FOLD for t in node.targets)
                and isinstance(node.value, ast.Dict)):
            elements = []
            for key, value in zip(node.value.keys, node.value.values):
                if key is not None:
                    elements.append(key)
                elements.append(value)
            return elements
    return []


def call_site_arguments():
    """What the caller passes to each of the builder's parameters.

    The builder names its inputs; the caller decides what goes into them. `include_superseded` is
    the whole of `include_superseded_resources or historical_replay` at the call site, so reading
    only the builder's tuple reports a divergence that is not one.
    """
    for node in ast.walk(_parse(CALLER_FILE)):
        if not isinstance(node, ast.Call):
            continue
        name = node.func.attr if isinstance(node.func, ast.Attribute) else getattr(
            node.func, "id", None)
        if name != EXTRACTED_KEY:
            continue
        return list(node.args) + [kw.value for kw in node.keywords]
    return []


def _covered_atoms():
    covered = set()
    for element in extracted_elements() + folded_into_ranking() + call_site_arguments():
        covered |= _atoms(element)
    return covered


def _divergent():
    covered = _covered_atoms()
    divergent = []
    for element in live_elements():
        atoms = _atoms(element)
        if not atoms:
            continue
        if not (atoms & covered):
            divergent.append((sorted(atoms)[0], ast.unparse(element)))
    return divergent


class TheExtractedPackCacheKeySeparatesWhatTheLiveOneDoes(unittest.TestCase):

    def test_the_scan_finds_both_keys(self):
        """A scan that matched nothing would report every discriminator covered."""
        live = live_elements()
        extracted = extracted_elements()
        folded = folded_into_ranking()
        self.assertGreaterEqual(
            len(live), 20,
            "found %d elements in the live %s tuple in %s, expected at least 20 -- the scan "
            "stopped matching, so the check below proves nothing"
            % (len(live), LIVE_KEY, LIVE_FILE))
        self.assertGreaterEqual(
            len(extracted), 8,
            "found %d elements in %s, expected at least 8" % (len(extracted), EXTRACTED_KEY))
        self.assertTrue(
            folded,
            "found no %s dict in %s -- the fold is most of what the extracted key actually "
            "carries, and without it this check reports divergences that are not real"
            % (CALLER_FOLD, CALLER_FILE))
        self.assertGreaterEqual(
            len(call_site_arguments()), 8,
            "found %d arguments at the %s call site in %s -- the builder names its inputs but the "
            "caller decides what goes into them, and without the call site this check reports "
            "divergences that are not real"
            % (len(call_site_arguments()), EXTRACTED_KEY, CALLER_FILE))

    def test_the_shared_discriminators_are_actually_shared(self):
        """The control: the ones both keys already agree on must read as covered.

        If this goes red the atom matching is broken, and a green result from the check below
        would mean nothing.
        """
        covered = _covered_atoms()
        for expected in ("query", "question_type", "retrieval_session_scope", "max_context_tokens",
                         "cache_scope_key", "ranking", "retrieval_records_cache_generation"):
            self.assertIn(expected, covered,
                          "%r is in both keys but the matching does not see it" % expected)

    def test_an_element_is_never_covered_by_punctuation_alone(self):
        """The negative control, and it has already fired once.

        Atoms include string constants, and `separators=(",", ":")` appears in nearly every element
        of both keys. Before those two were named plumbing, every dumped element intersected every
        other dumped element on a comma, so cross_session_policy -- absent from the extracted key
        entirely -- reported as covered. A short atom carries no meaning; a matcher that accepts
        one reports clean for the wrong reason.
        """
        short = sorted(a for a in _covered_atoms() if len(a) <= 2)
        self.assertEqual(
            [], short,
            "these atoms are too short to identify a discriminator, so anything containing one "
            "reads as covered: %s" % ", ".join(repr(a) for a in short))

    def test_every_live_discriminator_reaches_the_extracted_key(self):
        offenders = [
            "%s  (%s)" % (name, source[:70])
            for name, source in _divergent() if name not in RECORDED_DIVERGENCE
        ]
        self.assertEqual(
            [], offenders,
            "the live ContextPack cache key separates on these, the extracted one does not, so "
            "adopting the extraction would serve one cached pack to requests the live path keeps "
            "apart: %s" % "; ".join(offenders))

    def test_no_recorded_divergence_has_quietly_been_fixed(self):
        """Tighten in both directions: a recorded divergence that now IS covered must come off."""
        still = {name for name, _source in _divergent()}
        stale = sorted(set(RECORDED_DIVERGENCE) - still)
        self.assertEqual(
            [], stale,
            "these are recorded as divergences but the extracted key now carries them -- take "
            "them out of RECORDED_DIVERGENCE so the next one cannot hide behind them: %s"
            % ", ".join(stale))


if __name__ == "__main__":
    unittest.main()
