#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""One copy of each helper, and a list of the places that still have two.

A function copied into a second module does not stay a copy. `build_shared_context_policy` was
written out twice and the two drifted apart by a comment; `build_cross_session_policy` drifted
further, one copy matching a single profile-memory pattern where the other matched three, so which
behaviour a request got depended on which module its caller happened to import. Neither divergence
announced itself, because both copies answer for the ordinary input.

This is a ratchet, not a clean bill of health. `STILL_DUPLICATED` is the list of pairs that exist
today, and the suite fails if the tree grows a pair that is not on it. Fixing one means deleting its
line, which is what makes the progress visible in a diff rather than in a count nobody reads.

Bodies are compared as ASTs with docstrings dropped, so reformatting, renamed locals in a comment,
or a different quote style cannot hide a copy -- and a genuine DELEGATION (an import and a return)
is under the statement floor, so re-exporting is how you get off this list.
"""
from __future__ import annotations

import ast
import hashlib
import os
import subprocess
import unittest

TOOLS_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(TOOLS_DIR)

#: A body must have at least this many statements to count. Below it live delegations -- a
#: try/except import and a return -- and the one-line accessors that are shorter to repeat than to
#: import. Set to 4 because a delegation is exactly 2.
MIN_BODY_STATEMENTS = 4

#: Files whose duplication is not the same problem. Test modules repeat fixtures on purpose, and a
#: helper that exists to bootstrap importing cannot itself be imported from a shared module.
NON_PRODUCTION_PREFIXES = ("test_", "run_", "validate_")

#: Pairs that exist today, each with the reason it is still two copies. Delete a line when you
#: consolidate it; the suite fails if a line here is no longer duplicated, so the list cannot rot
#: into a set of claims nobody has checked.
#:
#: NONE of these is a copy to delete. Every one was tried: the two bodies are identical, but they
#: READ a name -- usually a module-scope constant -- that resolves to different code in each host,
#: so re-exporting swaps behaviour instead of removing a duplicate. The differing name is written
#: against each entry; that name is the decision, not the function.
#:
#: The constants are where the meaning sits, and comparing bodies alone does not see them. Two of
#: these were consolidated and reverted after the suite caught the change.
STILL_DUPLICATED = frozenset((
    # Blocked: reads `feature_profile_memory_budget_query`, local to both hosts and different.
    ('_default_memory_budget_mode', ('matrixark_mcp_local_adapter', 'matrixark_mcp_retrieve_pre_refresh')),
    # Blocked: bodies match, but they call different implementations of
    # `compact_context_index_postings`. matrixark_mcp_indexing buckets by `capability` and has an
    # adopt fast path (23.755 ms -> 3.255 ms on a 2,123-row cache); the one core republishes
    # buckets by `data_model`, has no adopt path, and reads only the singular node_hash. They
    # emit different posting_policy strings and different rows -- a data-format decision.
    ('compact_latest_context_state_records', ('matrixark_mcp_core_compact', 'matrixark_mcp_serving_records')),
    # Blocked: reads RESOURCE_FACT_SCHEMAS, local to both hosts and different in each.
    ('matched_resource_fact_schemas', ('matrixark_mcp_core', 'matrixark_mcp_resources')),
    # Blocked: resolves `resource_fact_entity_name` to two different implementations -- one
    # shortens with `preview_text`, the other with `summarize_text`. That changes the entity NAME
    # written for every resource fact.
    ('normalize_extracted_facts', ('matrixark_mcp_core_extraction', 'matrixark_mcp_extraction_normalization')),
    # Blocked: reads UNDERSTANDING_LABELS, which is a module-scope constant in BOTH hosts and
    # the two differ. Re-exporting would relabel what the encoder classifies.
    ('oss_encoder_compact_extraction', ('matrixark_mcp_core', 'matrixark_mcp_oss_understanding')),
    # Blocked: same UNDERSTANDING_LABELS divergence as oss_encoder_compact_extraction above.
    ('oss_encoder_event_type', ('matrixark_mcp_core', 'matrixark_mcp_oss_understanding')),
    # Blocked with the pair above -- same module, same constant, decided together or not at all.
    ('oss_encoder_rank_labels', ('matrixark_mcp_core', 'matrixark_mcp_oss_understanding')),
    # Blocked deliberately: keys its cache on `embedding_model_name()`, and the two hosts resolve
    # that to different implementations. test_string_defaults_agree records the disagreement as a
    # defect whose decision is open, because making the name agree relabels every vector a
    # populated store already holds. Backfill decision first.
    ('prototype_vectors', ('matrixark_mcp_core', 'matrixark_mcp_oss_understanding')),
    # Blocked, and the most serious of these: the two read different copies of
    # MATRIXARK_ROLE_SCOPE_LIMITS and disagree about an ACCESS DECISION --
    #
    #     matrixark_mcp_identity.role_allows_scopes('operator', {'context:forget'})   False
    #     matrixark_mcp_core_identity.role_allows_scopes(same)                        True
    #
    # core's copy carries 'context:forget' in the operator role and identity's does not. Both
    # live callers -- matrixark_access and matrixark_access_apikey -- reach core_identity, so an
    # operator MAY forget today. Consolidating picks one answer for a permission.
    ('role_allows_scopes', ('matrixark_mcp_core_identity', 'matrixark_mcp_identity')),
    # Blocked: reads SERVING_RESOURCE_METADATA_FIELDS, local to both hosts and different in each.
    # Consolidating this one put `content_hash` back into served metadata and was caught by
    # test_matrixark_content_hash_is_derived -- the constants, not the bodies, carry the meaning.
    ('serving_resource_metadata', ('matrixark_mcp_core', 'matrixark_mcp_resources')),
    # Blocked with the pair above.
    ('understanding_provider', ('matrixark_mcp_core', 'matrixark_mcp_oss_understanding')),
))


def _tracked_production_modules() -> list[str]:
    listed = subprocess.run(
        ["git", "ls-files", "tools/*.py"], cwd=REPO_ROOT,
        capture_output=True, text=True, check=False).stdout.split()
    out = []
    for rel in listed:
        base = os.path.basename(rel)
        if base.startswith(NON_PRODUCTION_PREFIXES):
            continue
        out.append(rel)
    return out


def _duplicate_pairs() -> set[tuple[str, tuple[str, ...]]]:
    """Every function body that appears at module scope in more than one production module."""
    by_shape: dict[str, list[tuple[str, str]]] = {}
    for rel in _tracked_production_modules():
        try:
            with open(os.path.join(REPO_ROOT, rel), encoding="utf-8", errors="replace") as handle:
                tree = ast.parse(handle.read())
        except (SyntaxError, OSError):
            continue
        for node in tree.body:
            if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            body = list(node.body)
            if body and isinstance(body[0], ast.Expr) and isinstance(body[0].value, ast.Constant) \
                    and isinstance(body[0].value.value, str):
                body = body[1:]          # a docstring is not an implementation
            if len(body) < MIN_BODY_STATEMENTS:
                continue
            shape = "\n".join(ast.dump(statement, include_attributes=False) for statement in body)
            digest = hashlib.sha256(shape.encode()).hexdigest()[:16]
            by_shape.setdefault(digest, []).append((os.path.basename(rel)[:-3], node.name))

    pairs: set[tuple[str, tuple[str, ...]]] = set()
    for members in by_shape.values():
        modules = tuple(sorted({module for module, _ in members}))
        if len(modules) < 2:
            continue                      # two defs in ONE module is a different fault
        pairs.add((sorted({name for _, name in members})[0], modules))
    return pairs


class OneCopyOfEachHelperTest(unittest.TestCase):

    def test_no_new_duplicate_appears(self) -> None:
        appeared = sorted(_duplicate_pairs() - STILL_DUPLICATED)
        self.assertEqual(
            [], appeared,
            "a function body was copied into a second module. Import it from the module that owns "
            "it, or delegate to it -- a delegation is two statements and drops off this list. "
            "New pairs:\n  " + "\n  ".join("%s in %s" % (n, " + ".join(m)) for n, m in appeared))

    def test_the_list_does_not_claim_a_pair_that_is_gone(self) -> None:
        """The half that keeps the list honest. Without it a consolidation leaves a line behind
        saying two copies remain, and the next reader trusts it."""
        fixed = sorted(STILL_DUPLICATED - _duplicate_pairs())
        self.assertEqual(
            [], fixed,
            "these are no longer duplicated -- delete them from STILL_DUPLICATED so the list keeps "
            "meaning what it says:\n  " + "\n  ".join(
                "%s in %s" % (n, " + ".join(m)) for n, m in fixed))

    def test_the_scan_actually_finds_things(self) -> None:
        """A floor. If the scan stopped parsing, or git ls-files returned nothing, both assertions
        above would pass while checking an empty tree."""
        modules = _tracked_production_modules()
        self.assertGreater(len(modules), 200, "the production module scan came back nearly empty")
        self.assertGreater(
            len(STILL_DUPLICATED), 0,
            "if this ever reaches zero, that is the goal -- replace this floor with an assertion "
            "that the set is empty, rather than deleting the suite")


if __name__ == "__main__":
    unittest.main()
