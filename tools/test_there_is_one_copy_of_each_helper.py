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


#: Names still compiled in more than one production module, each with the reason it is not one
#: definition yet. The holders are printed by the failure rather than listed here, so an entry does
#: not go stale when a copy moves.
STILL_COMPILED_TWICE = {
    # The six below are identical in every holder, which is what makes them look mechanical. They
    # are not, and it is the same obstruction for all six: the only holder that can own the name
    # without closing an import cycle is `matrixark_mcp_core`, so consolidating means every other
    # holder importing the aggregator. That dependence is already why
    # `matrixark_mcp_core_scoring`, `matrixark_mcp_core_candidate_policy`,
    # `matrixark_mcp_core_query_analysis` and `matrixark_mcp_core_codex_outcome` cannot be imported
    # unless the aggregator is imported first. Deepening it to delete a duplicate trades a copy for
    # a deadlock. They come off this list when the aggregator stops importing its own consumers
    # from its body, not before.
    "ACTIVE_MEMORY_GOAL_QUERY_RE": "consolidating needs an import of the aggregator",
    "CODEX_OUTCOME_QUERY_RE": "consolidating needs an import of the aggregator",
    "PROFILE_MEMORY_QUERY_RE": "consolidating needs an import of the aggregator",
    "PROFILE_MEMORY_STANDING_RULE_QUERY_RE": "consolidating needs an import of the aggregator",
    "FEATURE_SCOPE_EXCLUSION_RE": "same, and the hook copy differs deliberately -- see below",
    "FEATURE_SCOPE_EXCLUDED_DIMENSION_RE": "same, and the hook copy differs deliberately",
    # Both copies are live -- matrixark_mcp_core:should_extract_resource_fact and
    # matrixark_mcp_resources -- and they differ by one keyword: core matches `risk`, resources
    # matches `control_state`. Which is right depends on RESOURCE_FACT_SCHEMAS, which is ALSO
    # different in each host and is already recorded as a blocker in STILL_DUPLICATED above. The
    # keyword set and the schema set have to be settled together or a fact starts being extracted
    # with no schema to classify it.
    "RESOURCE_FACT_KEYWORDS": "diverged by one keyword; blocked on the same RESOURCE_FACT_SCHEMAS "
                              "split already recorded in STILL_DUPLICATED",
    # Two standalone scripts, neither importing the other, each tokenising its own input: one folds
    # case by matching lowercase only, the other keeps it. Not a shared rule that drifted -- a
    # short local constant that happens to share a name.
    "WORD_RE": "two unrelated scripts with their own tokenisers, not a shared rule",
}

#: Duplicates whose copies are MEANT to differ, with the reason. Listed separately from the
#: obstruction above because the agreement check below is what would otherwise force them to be
#: made the same -- and making them the same would be the defect.
#:
#: `matrixark_codex_hook` searches text that has been whitespace-normalised but not lowered
#: (`feature_scope_memory_only_policy`), so its copies carry re.IGNORECASE. Every other holder
#: searches a string the caller already lowered, where the flag changes nothing. Same rule, two
#: call conventions.
DELIBERATELY_UNLIKE = frozenset((
    "FEATURE_SCOPE_EXCLUSION_RE",
    "FEATURE_SCOPE_EXCLUDED_DIMENSION_RE",
    "RESOURCE_FACT_KEYWORDS",
    "WORD_RE",
))


def _compiled_patterns() -> dict:
    """name -> {module: (pattern source, flag source)} for every module-scope `X = re.compile(...)`.

    Compared on SOURCE, not on the compiled object. `re.compile` caches on (pattern, flags), so two
    modules compiling the same text are handed the SAME object back -- identity here would report
    every duplicate as one shared definition and see nothing at all.

    Flags are read from the keyword form as well as the positional one. `re.compile(p, re.I)` and
    `re.compile(p, flags=re.I)` are the same call, and reading only the first would have compared a
    case-insensitive copy equal to a case-sensitive one -- which is precisely the divergence this
    is for.
    """
    found: dict = {}
    for rel in _tracked_production_modules():
        try:
            with open(os.path.join(REPO_ROOT, rel), encoding="utf-8", errors="replace") as handle:
                tree = ast.parse(handle.read())
        except (SyntaxError, OSError):
            continue
        for node in tree.body:
            if not isinstance(node, ast.Assign) or len(node.targets) != 1:
                continue
            target, call = node.targets[0], node.value
            if not isinstance(target, ast.Name):
                continue
            if not (isinstance(call, ast.Call) and isinstance(call.func, ast.Attribute)
                    and call.func.attr == "compile" and call.args):
                continue
            try:
                pattern = ast.literal_eval(call.args[0])
            except (ValueError, SyntaxError):
                continue                  # built at runtime: not a constant to duplicate
            flags = "|".join(
                sorted([ast.dump(a, include_attributes=False) for a in call.args[1:]]
                       + [ast.dump(k.value, include_attributes=False) for k in call.keywords
                          if k.arg == "flags"]))
            found.setdefault(target.id, {})[os.path.basename(rel)[:-3]] = (pattern, flags)
    return found


def _patterns_with_several_copies() -> dict:
    return {name: holders for name, holders in _compiled_patterns().items() if len(holders) > 1}


#: Constants defined at module scope in more than one LIVE module, whose copies AGREE today.
#: A copy too many, not a split -- listed so a NEW one has to be looked at rather than joining a
#: count nobody reads.
#: The nine record-shape constants that matrixark_mcp_core_compact and
#: matrixark_mcp_serving_records both declared are gone from here: serving_records owns them
#: now and core_compact re-exports, so the list is shorter by exactly what was consolidated.
LIVE_DUPLICATE_CONSTANTS = frozenset((
    "AUTO_BUDGET_QUERY_TYPES",
    "DEFAULT_BUSINESS_TYPE_WEIGHTS",
    "RESOURCE_EVENTS",
    "RESOURCE_TYPE_BY_SUFFIX",
    "STORAGE_ROUTE_PRESETS",
    "_API_EMBEDDING_PROVIDERS",
    "_OSS_EMBEDDING_PROVIDERS",
))


def _unreachable_modules() -> set:
    """The modules only the tests reach, taken from the guard that maintains that list.

    Imported rather than copied. A second list of forty-three module names would be the exact
    fault this file exists to bound, and the two would drift the way everything else here did.
    """
    import importlib.util

    path = os.path.join(TOOLS_DIR, "test_a_module_only_tests_reach_is_not_live.py")
    spec = importlib.util.spec_from_file_location("_reachability_list", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    out: set = set()
    for members in module.UNREACHABLE.values():
        out.update(members)
    return out


def _data_constants() -> dict:
    """name -> {module: repr of the value} for module-scope container literals.

    Containers only, and only those with more than one member: a scalar or a one-item container is
    shorter to repeat than to import, and reporting those would bury the rules in noise. Values are
    compared as evaluated literals, so key order and formatting do not count as a difference.
    """
    found: dict = {}
    for rel in _tracked_production_modules():
        try:
            with open(os.path.join(REPO_ROOT, rel), encoding="utf-8", errors="replace") as handle:
                tree = ast.parse(handle.read())
        except (OSError, SyntaxError):
            continue
        for node in tree.body:
            if isinstance(node, ast.Assign) and len(node.targets) == 1:
                target, value = node.targets[0], node.value
            elif isinstance(node, ast.AnnAssign) and node.value is not None:
                target, value = node.target, node.value
            else:
                continue
            if not isinstance(target, ast.Name) or not target.id.isupper():
                continue
            if isinstance(value, ast.Call):
                func = value.func
                builder = getattr(func, "id", getattr(func, "attr", ""))
                if builder not in ("frozenset", "set", "dict", "tuple", "list") \
                        or len(value.args) != 1:
                    continue
                value = value.args[0]
            elif not isinstance(value, (ast.Dict, ast.Set, ast.List, ast.Tuple)):
                continue
            try:
                literal = ast.literal_eval(value)
            except (ValueError, SyntaxError, TypeError):
                continue
            if len(literal) < 2:
                continue
            shape = sorted(literal) if isinstance(literal, (set, frozenset)) else literal
            found.setdefault(target.id, {})[os.path.basename(rel)[:-3]] = repr(shape)
    return found


def _constants_duplicated_between_live_modules() -> dict:
    """name -> {module: value} for constants held by more than one module a request can reach."""
    unreachable = _unreachable_modules()
    out = {}
    for name, holders in _data_constants().items():
        live = {mod: value for mod, value in holders.items() if mod not in unreachable}
        if len(live) > 1:
            out[name] = live
    return out


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

    def test_no_rule_is_compiled_in_a_module_that_is_not_listed(self) -> None:
        """The ratchet direction: a new second copy of a pattern fails here.

        A rule copied into a second module is the shape that already produced a live defect.
        `FEATURE_MEMORY_QUERY_RE` was compiled twice, one copy matching thirty topics and the
        other nineteen, and the function ratchet next door could not see it -- a compiled pattern
        is an assignment, not a body, so a rule can drift by eleven alternatives without a single
        duplicate function existing."""
        several = _patterns_with_several_copies()
        unlisted = sorted(set(several) - set(STILL_COMPILED_TWICE))
        detail = ["%s in %s" % (name, ", ".join(sorted(several[name]))) for name in unlisted]
        self.assertEqual(
            [], detail,
            "a pattern is now compiled in more than one module; import it from the one that owns "
            "it, or add it to STILL_COMPILED_TWICE with the reason it cannot be")

    def test_the_list_does_not_claim_a_copy_that_is_gone(self) -> None:
        """Tight in the other direction too. A list that keeps names after they are fixed stops
        being a record of what is left and becomes a place entries go to be forgotten."""
        several = _patterns_with_several_copies()
        stale = sorted(name for name in STILL_COMPILED_TWICE if name not in several)
        self.assertEqual(
            [], stale,
            "these names have one definition now; strike them from STILL_COMPILED_TWICE")

    def test_a_tolerated_duplicate_has_not_quietly_drifted(self) -> None:
        """The copies above are tolerated because they agree. Drift is the actual hazard, so if one
        stops agreeing the reason it is tolerated has stopped holding -- and nothing else in the
        tree would say so."""
        several = _patterns_with_several_copies()
        drifted = []
        for name in sorted(set(STILL_COMPILED_TWICE) - DELIBERATELY_UNLIKE):
            holders = several.get(name, {})
            if len(set(holders.values())) > 1:
                drifted.append("%s: %s" % (name, ", ".join(sorted(holders))))
        self.assertEqual(
            [], drifted,
            "a tolerated duplicate has diverged; its copies now answer differently, which is the "
            "fault this list exists to bound")

    def test_the_deliberate_differences_are_still_deliberate(self) -> None:
        """The other half of the check above. A name is excused from agreeing only while it really
        does differ -- once the copies converge the excuse is stale, and leaving it in place would
        silently exempt a name that has rejoined the rule."""
        several = _patterns_with_several_copies()
        converged = []
        for name in sorted(DELIBERATELY_UNLIKE):
            holders = several.get(name, {})
            if holders and len(set(holders.values())) == 1:
                converged.append(name)
        self.assertEqual(
            [], converged,
            "these copies are identical now, so listing them as deliberately unlike is wrong; "
            "strike them from DELIBERATELY_UNLIKE and consolidate them")

    def test_the_pattern_scan_actually_finds_things(self) -> None:
        """A floor under the four above. If the scan stopped matching `re.compile`, every one of
        them would pass against an empty result."""
        found = _compiled_patterns()
        self.assertGreater(
            len(found), 25,
            "the pattern scan came back nearly empty, so an empty duplicate set says nothing")
        self.assertIn(
            "PROFILE_MEMORY_QUERY_RE", found,
            "a name known to be compiled in several modules is missing from the scan")

    def test_the_scan_reads_flags_passed_by_keyword(self) -> None:
        """A floor under the drift checks specifically.

        `re.compile(p, flags=re.IGNORECASE)` and `re.compile(p, re.IGNORECASE)` are the same call.
        An earlier version of this scan read positional arguments only, so a case-insensitive copy
        compared EQUAL to a case-sensitive one and the drift checks passed over the divergence they
        exist to find."""
        found = _compiled_patterns()
        keyword_form = found.get("RESOURCE_FACT_KEYWORDS", {}).get("matrixark_mcp_resources")
        self.assertIsNotNone(keyword_form, "the keyword-flag sample is gone; pick another")
        self.assertTrue(
            keyword_form[1],
            "a pattern compiled with `flags=` came back with no flags recorded, so a difference "
            "in case sensitivity would read as identical")

    def test_no_constant_disagrees_with_itself_between_live_modules(self) -> None:
        """The sharp one. Two live modules holding one name with DIFFERENT values is two answers to
        one question, with a caller on each.

        Every instance found so far was silent, and two of them changed what a request got:
        MATRIXARK_TOOL_SCOPES was missing eleven tools in one copy -- and a tool absent from that
        map has NO scope requirement, not a stricter one -- and
        SECONDARY_INDEX_PRIORITY_PREFIXES was missing three kinds the indexer emits, so on the
        live path they sorted as unrecognised and were dropped first when the term limit bit.

        Constants whose second copy is in a module only tests reach are out of scope here: that is
        a different problem, already recorded by the guard that owns the reachability list, and
        picking a winner between a live copy and an unreachable one is how a stale copy gets
        promoted."""
        split = {}
        for name, live in _constants_duplicated_between_live_modules().items():
            if len(set(live.values())) > 1:
                split[name] = sorted(live)
        self.assertEqual(
            {}, split,
            "these constants are defined in two live modules with different values, so which "
            "answer a request gets depends on which module served it: %r" % (split,))

    def test_no_new_constant_is_duplicated_between_live_modules(self) -> None:
        """The ratchet direction. A second copy in another live module agrees on the day it is
        written; that is what makes it look harmless."""
        duplicated = set(_constants_duplicated_between_live_modules())
        unlisted = sorted(duplicated - LIVE_DUPLICATE_CONSTANTS)
        self.assertEqual(
            [], unlisted,
            "these constants are now defined in more than one live module; import from the one "
            "that owns the rule, or add them to LIVE_DUPLICATE_CONSTANTS")

    def test_the_constant_list_does_not_claim_a_copy_that_is_gone(self) -> None:
        """Tight in the other direction, so the list stays a record of what is left."""
        duplicated = set(_constants_duplicated_between_live_modules())
        stale = sorted(LIVE_DUPLICATE_CONSTANTS - duplicated)
        self.assertEqual(
            [], stale,
            "these have one live definition now; strike them from LIVE_DUPLICATE_CONSTANTS")

    def test_the_constant_scan_actually_finds_things(self) -> None:
        """A floor under the three above, and under the reachability list they lean on."""
        found = _data_constants()
        self.assertGreater(
            len(found), 150,
            "the constant scan came back nearly empty, so an empty split set says nothing")
        self.assertGreater(
            len(_unreachable_modules()), 30,
            "the reachability list came back nearly empty, so every duplicate would count as "
            "live-vs-live and the split check would be about the wrong set")
        self.assertIn(
            "STORAGE_ROUTE_PRESETS", found,
            "a constant known to be duplicated between live modules is missing from the scan")

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
