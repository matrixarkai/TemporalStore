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

#: Empty, and meant to stay that way. It held thirty-three pairs: every one is now a single
#: definition that the other module re-exports or delegates to.
#:
#: Getting here needed more than moving functions. Most pairs had identical bodies but READ a
#: name -- usually a module-scope constant -- that resolved to different code in each host, so
#: re-exporting would have swapped behaviour rather than removed a copy. Those had to be
#: consolidated first, and which copy survived was decided by evidence, not preference:
#: `git log -S` for which one a change actually landed in, and the reachability record for
#: which module production can get to at all.
#:
#: If a pair ever needs to go back on this list, put the REASON beside it. A bare name invites
#: the next reader to redo the analysis, or to consolidate it because it looks mechanical.
STILL_DUPLICATED = frozenset((
    # Blocked: reads `feature_profile_memory_budget_query`, local to both hosts and different.
    # Blocked: bodies match, but they call different implementations of
    # `compact_context_index_postings`. matrixark_mcp_indexing buckets by `capability` and has an
    # adopt fast path (23.755 ms -> 3.255 ms on a 2,123-row cache); the one core republishes
    # buckets by `data_model`, has no adopt path, and reads only the singular node_hash. They
    # emit different posting_policy strings and different rows -- a data-format decision.
    # Blocked: reads RESOURCE_FACT_SCHEMAS, local to both hosts and different in each.
    # Blocked: resolves `resource_fact_entity_name` to two different implementations -- one
    # shortens with `preview_text`, the other with `summarize_text`. That changes the entity NAME
    # written for every resource fact.
    # Blocked: reads UNDERSTANDING_LABELS, which is a module-scope constant in BOTH hosts and
    # the two differ. Re-exporting would relabel what the encoder classifies.
    # Blocked: same UNDERSTANDING_LABELS divergence as oss_encoder_compact_extraction above.
    # Blocked with the pair above -- same module, same constant, decided together or not at all.
    # Blocked deliberately: keys its cache on `embedding_model_name()`, and the two hosts resolve
    # that to different implementations. test_string_defaults_agree records the disagreement as a
    # defect whose decision is open, because making the name agree relabels every vector a
    # populated store already holds. Backfill decision first.
    # Blocked, and the most serious of these: the two read different copies of
    # MATRIXARK_ROLE_SCOPE_LIMITS and disagree about an ACCESS DECISION --
    #
    #     matrixark_mcp_identity.role_allows_scopes('operator', {'context:forget'})   False
    #     matrixark_mcp_core_identity.role_allows_scopes(same)                        True
    #
    # core's copy carries 'context:forget' in the operator role and identity's does not. Both
    # live callers -- matrixark_access and matrixark_access_apikey -- reach core_identity, so an
    # operator MAY forget today. Consolidating picks one answer for a permission.
    # Blocked: reads SERVING_RESOURCE_METADATA_FIELDS, local to both hosts and different in each.
    # Consolidating this one put `content_hash` back into served metadata and was caught by
    # test_matrixark_content_hash_is_derived -- the constants, not the bodies, carry the meaning.
    # Blocked with the pair above.
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


def _parameters(node) -> dict:
    """name -> default source text (None when the parameter is required)."""
    args = node.args
    positional = args.posonlyargs + args.args
    out: dict = {}
    pad = [None] * (len(positional) - len(args.defaults)) + list(args.defaults)
    for arg, default in zip(positional, pad):
        out[arg.arg] = None if default is None else " ".join(ast.unparse(default).split())
    for arg, default in zip(args.kwonlyargs, args.kw_defaults):
        out[arg.arg] = None if default is None else " ".join(ast.unparse(default).split())
    return out


def _keywords_forwarded(node) -> set:
    """Keyword names the delegation passes explicitly in its forwarding call."""
    supplied = set()
    for sub in ast.walk(node):
        if isinstance(sub, ast.Call):
            for kw in sub.keywords:
                if kw.arg:
                    supplied.add(kw.arg)
            if any(kw.arg is None for kw in sub.keywords):
                supplied.add("**")
    return supplied


def _delegation_target(node):
    """The module a function forwards to, or None if it is an implementation.

    Same shape the floor above draws: an import and a return, plus the `global`/cache lines a
    memoised delegation adds. Anything longer is a body of its own.
    """
    body = [b for b in node.body
            if not (isinstance(b, ast.Expr) and isinstance(b.value, ast.Constant))]
    if not body or len(body) > 6:
        return None
    if not any(isinstance(b, ast.Return) for b in body):
        return None
    if any(isinstance(b, (ast.For, ast.While, ast.With)) for b in body):
        return None
    for sub in ast.walk(node):
        if isinstance(sub, ast.ImportFrom) and sub.module:
            for alias in sub.names:
                if alias.name == node.name:
                    return sub.module.rsplit(".", 1)[-1]
    return None


def _parameters_of(module: str, name: str):
    """The parameters of `name` as `module` defines it, or None if it does not define it."""
    path = os.path.join(TOOLS_DIR, module + ".py")
    try:
        with open(path, encoding="utf-8", errors="replace") as fh:
            tree = ast.parse(fh.read())
    except (SyntaxError, OSError):
        return None
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == name:
            return _parameters(node)
    return None


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

    def test_no_production_helper_is_written_twice(self) -> None:
        """The assertion the floor below used to stand in for.

        STILL_DUPLICATED is empty: every one of the thirty-three pairs it once listed is now a
        single definition, re-exported or delegated to. So this stops being a ratchet on a number
        and becomes the plain statement -- there is one copy of each.
        """
        self.assertEqual(
            frozenset(), STILL_DUPLICATED,
            "the list is meant to be empty now; a pair here means someone re-listed one instead "
            "of consolidating it")
        self.assertEqual(
            set(), _duplicate_pairs(),
            "a production helper is written twice again:\n  " + "\n  ".join(
                "%s in %s" % (n, " + ".join(m)) for n, m in sorted(_duplicate_pairs())))

    def test_a_delegation_keeps_the_signature_it_stands_in_front_of(self) -> None:
        """A delegation is a hand-written signature in front of someone else's.

        Consolidating replaces a body with a forwarding call, and the parameter list has to be
        copied across. `feature_profile_memory_budget_query` lost `question_type: str = "fact"`
        that way: the wrapper imported cleanly, forwarded correctly and read as obviously right,
        and every caller that relied on the default raised TypeError instead.

        Compared as text after normalising whitespace, because a default that is a call or a
        literal cannot be compared by value without importing both modules -- and importing every
        module in tools/ is what this suite is careful not to need.
        """
        wrong = []
        for rel in _tracked_production_modules():
            try:
                with open(os.path.join(REPO_ROOT, rel), encoding="utf-8", errors="replace") as fh:
                    source = fh.read()
                tree = ast.parse(source)
            except (SyntaxError, OSError):
                continue
            for node in tree.body:
                if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    continue
                target = _delegation_target(node)
                if target is None:
                    continue
                theirs = _parameters_of(target, node.name)
                if theirs is None:
                    continue
                mine = _parameters(node)
                supplied = _keywords_forwarded(node)
                for name, default in mine.items():
                    if name not in theirs:
                        wrong.append("%s.%s exposes `%s`, which %s does not take"
                                     % (os.path.basename(rel)[:-3], node.name, name, target))
                    elif theirs[name] != default:
                        wrong.append(
                            "%s.%s declares `%s=%s` where %s declares `%s=%s` -- a caller "
                            "relying on the default gets a different value, or none"
                            % (os.path.basename(rel)[:-3], node.name, name, default,
                               target, name, theirs[name]))
                for name, default in theirs.items():
                    if name in mine or name in supplied or default is not None:
                        continue
                    wrong.append(
                        "%s.%s neither exposes nor supplies `%s`, which %s requires -- the "
                        "forward raises TypeError"
                        % (os.path.basename(rel)[:-3], node.name, name, target))
        self.assertEqual(
            [], wrong,
            "a delegation's parameter list has drifted from the implementation it forwards to. "
            "A caller that relies on a default the wrapper dropped raises TypeError, and nothing "
            "about the wrapper looks wrong:\n  " + "\n  ".join(wrong))

    def test_the_scan_actually_finds_things(self) -> None:
        """A floor. If the scan stopped parsing, or git ls-files returned nothing, every assertion
        above would pass while checking an empty tree -- and now that the expected answer IS empty,
        that is the only thing keeping them honest."""
        modules = _tracked_production_modules()
        self.assertGreater(len(modules), 200, "the production module scan came back nearly empty")
        bodies = 0
        for rel in modules:
            with open(os.path.join(REPO_ROOT, rel), encoding="utf-8", errors="replace") as handle:
                bodies += handle.read().count("\ndef ")
        self.assertGreater(
            bodies, 2000,
            "the scan read the files but found almost no functions in them, so an empty result "
            "says nothing")


if __name__ == "__main__":
    unittest.main()
