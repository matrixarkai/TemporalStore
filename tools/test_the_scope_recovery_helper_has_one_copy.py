#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Two retrieval paths recover a record's access scope differently, and no guard could see it.

`recovered_record_scope` decides which tenant, user and session a stored record belongs to when
the record does not carry a scope of its own. It is defined twice, nested both times:

    matrixark_local_adapter_retrieval   inside retrieval_records   11 statements
    matrixark_local_adapter_retrieve    inside retrieve             8 statements

The three statements the shorter copy lacks are one block. For a record of type
`context_event`, `context_entity`, `context_segment`, `context_compression_event` or
`context_summary` that carries no scope, the longer copy looks the record's owner up in
`embedding_scope_by_ref` -- a map from (ref type, ref hash) to the scope recovered from that
record's embedding -- and returns it. The shorter copy has no such branch, and
`embedding_scope_by_ref` is a name that does not appear ANYWHERE in its module.

WHICH DIRECTION IS MISSING, stated precisely, because "the mechanism is absent" was too coarse.
Both modules build a ref-keyed scope map and both build it with the same five ref types and the
same hash fields. They differ in DIRECTION:

    ref_scope_by_key          owner record -> its scope.  Built by BOTH modules, and consulted
                              for a `context_embedding`, to give an embedding its owner's scope.
    embedding_scope_by_ref    embedding -> its scope.  Built ONLY by the longer module, and
                              consulted for an OWNER, to give an owner its embedding's scope.

So the shorter module is not missing scope recovery. It is missing the embedding-to-owner
direction of it. It also builds its maps from a different population: the longer module walks
`self.read_all()`, the whole store; the shorter walks the records its own upstream call already
returned.

DOES THE SHORTER COPY'S CALLER EVER NEED IT? Asked because an absence that the caller never
reaches is correct rather than behind, and the first version of this record implied a missing
feature without checking. The chain, read rather than executed, and labelled as such:

  * `retrieve` gets its records from `self.retrieval_records(...)` -- the function the LONGER copy
    lives in (`records = retrieval_record_result["records"]`).
  * `retrieval_records` gates owner records through `recovered_scope_matches`, which is where the
    longer copy's extra block does its work; one of those gates is spelled `owner_record`.
  * That gate produces a BOOL. `recovered_scope_for_query` is never written back onto the record,
    so a record admitted because its embedding's scope was recoverable arrives downstream still
    carrying no scope of its own.
  * `retrieve` then re-gates those same records through its OWN `recovered_scope_matches` at nine
    sites, most of them `if not recovered_scope_matches(record, scope): continue`.

MEASURED, and this is the part that decides the direction of the consequence: an unrecovered scope
does not fall through as permissive. `scope_matches({}, query)` is False for every non-empty query
tried -- the retrieval default `prefer`, `only`, and a tenant-only query -- and True only for an
empty query. So at those nine gates an unrecovered record is DROPPED, not kept.

WHAT IS THEREFORE CLAIMED, and what is not. Claimed, and asserted below: the two copies resolve
differently for the same record, and an unresolved scope is a drop rather than a pass. NOT claimed:
that a live corpus contains such a record. The shorter copy keeps two further fallbacks -- a
`node_hash` shared with an already-scoped record in the filtered set, and a `node_path` carrying
`tenant:`/`user:`/`session:` -- so the loss needs a record that reaches none of them. Establishing
that requires a live retrieve against a real store, which this file does not do.

MEASURED BY EXECUTION. A nested function cannot be imported, so each copy is lifted out of its own
module's source by AST and compiled against the same synthetic enclosing scope, using the real
`candidate_access_scope` and the real `scope_from_node_path` -- the latter now read from the one
module that publishes it. The bodies are the tree's, unchanged.

    record with no scope of its own      retrieval copy     retrieve copy
    ---------------------------------    ---------------    -------------
    context_event                        the owner scope    {}
    context_entity                       the owner scope    {}
    context_segment                      the owner scope    {}
    context_compression_event            the owner scope    {}
    context_summary                      the owner scope    {}
    context_summary keyed by node_hash   the owner scope    {}

An empty scope is not a harmless default here: `scope_matches` and the access-scope rules read it,
so the two paths disagree about who a record belongs to. Every branch the two copies SHARE agrees
on every input tried -- own scope, `embedding_meta`, the `context_embedding` lookup, `node_hash`,
`node_path` -- so the divergence is that block and nothing else. That control is asserted below,
because a difference that turned out to be somewhere else would make the record wrong.

WHY NOTHING CAUGHT IT, which is the part worth keeping.

`test_a_nested_helper_has_one_copy_too` exists for exactly this shape and ALREADY records this
module pair. It groups functions by the exact unparsed text of their bodies, so it finds copies
that are still IDENTICAL -- for this pair it reports `profile_summary_path_matches` and
`profile_summary_scope_matches`, byte for byte the same under two different names. It used to
report `scope_from_node_path` as well, until that one was consolidated to a single
implementation.
`recovered_record_scope` sits in the same two enclosing functions, three lines from one of them,
and does not group at all, because the copies have stopped agreeing.

That is the inverse of what you want from a duplicate guard: it catches the copies that still
agree, and goes blind at the moment one of them changes. It is the same blind spot recorded for
the definition-level orphan scan, which cannot see an orphan that is a duplicate because the other
module's own `def` line seeds the name as reached -- a scan keyed on sameness stops seeing a thing
the moment it differs. This file asserts that blind spot rather than describing it, so if the
older guard ever grows drift matching, this record is told to move.

The module-level guards cannot see it either: `test_there_is_one_copy_of_each_helper` walks
`tree.body`, and both copies are nested.

THIS FILE DOES NOT ASSERT THAT THE COPIES AGREE. Giving the shorter copy the block widens which
records a query on that path can resolve a scope for, and an access scope that resolves where it
used to come back empty changes who can see what. That is a decision. Recorded in both directions:
a copy that stops diverging fails here and asks which side won.
"""
from __future__ import annotations

import ast
import hashlib
import importlib
import os
import sys
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
if TOOLS not in sys.path:
    sys.path.insert(0, TOOLS)

HELPER = "recovered_record_scope"

#: (module stem, the function the helper is nested in, its statement count today).
COPIES = (
    ("matrixark_local_adapter_retrieval", "retrieval_records", 11),
    ("matrixark_local_adapter_retrieve", "retrieve", 8),
)
LONGER, SHORTER = COPIES[0][0], COPIES[1][0]

#: Deduplicated: defined once, in SHORTER, and imported by LONGER.
NODE_PATH_HELPER = "scope_from_node_path"

#: The embedding-to-owner direction: built and consulted only by the longer module.
OWNER_MAP = "embedding_scope_by_ref"

#: The owner-to-embedding direction: built by BOTH modules, consulted for a context_embedding.
#: Named here so the record says which half is shared and which half is missing.
OTHER_DIRECTION_MAP = "ref_scope_by_key"

SCOPE = {"tenant_hash": 111, "user_hash": 222, "session_hash": 333, "agent_hash": 444}

#: One record per ref type the extra block knows about, keyed to match OWNER_MAP below.
OWNED_RECORDS = {
    "context_event": {"record_type": "context_event", "event_id_hash": 9001},
    "context_entity": {"record_type": "context_entity", "entity_hash": 9002},
    "context_segment": {"record_type": "context_segment", "segment_hash": 9003},
    "context_compression_event": {"record_type": "context_compression_event",
                                  "compression_id_hash": 9004},
    "context_summary": {"record_type": "context_summary", "summary_hash": 9005},
    # The longer copy falls back to node_hash when a summary carries no summary_hash.
    "context_summary keyed by node_hash": {"record_type": "context_summary", "node_hash": 9005},
}

OWNER_SCOPES = {("event", 9001): dict(SCOPE), ("entity", 9002): dict(SCOPE),
                ("segment", 9003): dict(SCOPE), ("compression", 9004): dict(SCOPE),
                ("summary", 9005): dict(SCOPE)}

#: Records that take a branch both copies have. Asserted to AGREE.
SHARED_BRANCH_RECORDS = {
    "carries its own scope": {"record_type": "context_summary", "scope": dict(SCOPE)},
    "scope under embedding_meta": {"record_type": "context_summary",
                                   "embedding_meta": {"scope": dict(SCOPE)}},
    "context_embedding through the ref map": {"record_type": "context_embedding",
                                              "ref_type": "summary", "ref_hash": 1},
    "falls through to node_hash": {"record_type": "context_pipeline_task", "node_hash": 7},
    "falls through to node_path": {
        "record_type": "context_pipeline_task",
        "node_path": ["tenant:111", "user:222", "session:333"]},
}


def _source(stem):
    with open(os.path.join(TOOLS, stem + ".py"), encoding="utf-8", errors="replace") as handle:
        return handle.read()


def _nested_definition(stem, enclosing):
    """The helper's AST node, found inside the function it is nested in."""
    for node in ast.walk(ast.parse(_source(stem))):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == enclosing:
            for inner in ast.walk(node):
                if isinstance(inner, ast.FunctionDef) and inner.name == HELPER:
                    return inner
    return None


def _body(node):
    body = node.body
    if (body and isinstance(body[0], ast.Expr) and isinstance(body[0].value, ast.Constant)
            and isinstance(body[0].value.value, str)):
        body = body[1:]  # prose is not behaviour
    return body


def _digest(node):
    return hashlib.sha256(
        ast.dump(ast.Module(body=_body(node), type_ignores=[])).encode("utf-8")).hexdigest()[:12]


def _binds(stem, name):
    """Is `name` BOUND anywhere in this module, rather than merely mentioned?

    The Store context is the whole point: `m[k] = v` is an assignment whose target holds a Name
    for `m`, but that Name is a READ of a map built somewhere else. Counting it would let the
    declaration be renamed away while this still answered yes.
    """
    for node in ast.walk(ast.parse(_source(stem))):
        targets = []
        if isinstance(node, ast.Assign):
            targets = node.targets
        elif isinstance(node, (ast.AnnAssign, ast.AugAssign)):
            targets = [node.target]
        for target in targets:
            for inner in ast.walk(target):
                if (isinstance(inner, ast.Name) and inner.id == name
                        and isinstance(inner.ctx, ast.Store)):
                    return True
    return False


def _mentions(stem, name):
    """Does `name` appear as an identifier anywhere in this module?"""
    return any(isinstance(node, ast.Name) and node.id == name
               for node in ast.walk(ast.parse(_source(stem))))


def _import(stem):
    """Import a tree module under whichever spelling this run is already using.

    Under `unittest discover` a module is reachable as both `tools.X` and bare `X`, and those are
    DIFFERENT module objects. Asking for one spelling by name is not a neutral act: it either
    finds what the run already loaded, or it loads a second copy.
    """
    try:
        return importlib.import_module("tools." + stem)
    except ImportError:  # Direct script execution from tools/.
        return importlib.import_module(stem)


def _retrieve_module():
    """The retrieve module, as THIS run has it.

    It cannot be imported standalone -- it resolves a cycle through its parent's import order --
    so the parent is imported first and then whichever spelling landed is used.

    This used to read `sys.modules["matrixark_local_adapter_retrieve"]` by that exact name, and
    that is what broke the suite. When something earlier in a discovery run has already imported
    `tools.matrixark_mcp_local_adapter`, only the `tools.`-prefixed children are in `sys.modules`;
    the bare import of the parent then resolves ITS children through the package path too, so the
    bare child name is never created and the lookup raised KeyError. The module passed alone and
    failed in the suite, which is the signature of exactly this mistake.
    """
    _import("matrixark_mcp_local_adapter")
    for name in ("tools.matrixark_local_adapter_retrieve", "matrixark_local_adapter_retrieve"):
        module = sys.modules.get(name)
        if module is not None and hasattr(module, "scope_from_node_path"):
            return module
    raise AssertionError(
        "neither spelling of matrixark_local_adapter_retrieve is loaded after importing its "
        "parent, so this file cannot read the helper it compares against")


def _closure():
    """The enclosing scope both copies read, built from the tree's own functions."""
    access_scope = _import("matrixark_mcp_access_scope")
    retrieve_module = _retrieve_module()
    return {
        "candidate_access_scope": access_scope.candidate_access_scope,
        "scope_from_node_path": retrieve_module.scope_from_node_path,
        # The context_embedding lookup, which BOTH copies have. Populated so the shared-branch
        # control below actually reaches it rather than agreeing on an empty answer.
        "ref_scope_by_key": {("summary", 1): dict(SCOPE)},
        OWNER_MAP: dict(OWNER_SCOPES),
        "node_scope_by_hash": {7: dict(SCOPE)},
        "Json": dict,
    }


def _compiled(stem, enclosing, closure):
    """Run this module's copy, lifted out of its enclosing function."""
    node = _nested_definition(stem, enclosing)
    module = ast.Module(body=[node], type_ignores=[])
    ast.fix_missing_locations(module)
    environment = dict(closure)
    exec(compile(module, "<%s>" % stem, "exec"), environment)  # noqa: S102 - the tree's own source
    return environment[HELPER]


class TheScopeRecoveryHelperHasOneCopy(unittest.TestCase):

    def test_both_copies_are_there_to_lift(self) -> None:
        """A floor. Every assertion below passes over a helper that was not found."""
        for stem, enclosing, statements in COPIES:
            with self.subTest(module=stem):
                node = _nested_definition(stem, enclosing)
                self.assertIsNotNone(
                    node,
                    "%s is no longer nested inside %s in %s. If it was hoisted to module level, "
                    "the module-level guards can see it and this file should move there"
                    % (HELPER, enclosing, stem))
                self.assertEqual(
                    statements, len(_body(node)),
                    "%s's copy is now %d statements, recorded as %d -- the record below describes "
                    "a body that has changed" % (stem, len(_body(node)), statements))

    def test_the_two_bodies_still_differ(self) -> None:
        """The record itself, in the direction that fires when somebody picks a winner."""
        digests = {stem: _digest(_nested_definition(stem, enclosing))
                   for stem, enclosing, _n in COPIES}
        self.assertEqual(
            2, len(set(digests.values())),
            "the two copies of %s now have the same body. That is the fix -- delete this file and "
            "the record with it" % HELPER)

    def test_only_one_copy_recovers_a_scope_from_the_owner_map(self) -> None:
        """The measured table, both directions."""
        closure = _closure()
        longer = _compiled(COPIES[0][0], COPIES[0][1], closure)
        shorter = _compiled(COPIES[1][0], COPIES[1][1], closure)
        for label, record in OWNED_RECORDS.items():
            with self.subTest(record=label):
                recovered = longer(dict(record))
                missed = shorter(dict(record))
                self.assertEqual(
                    SCOPE, recovered,
                    "%s no longer recovers a %s's scope from %s, so this fixture no longer "
                    "separates the two copies" % (LONGER, label, OWNER_MAP))
                self.assertEqual(
                    {}, missed,
                    "%s now resolves a scope for a %s. That WIDENS which records a query on that "
                    "path can place; strike this record and say the decision was taken"
                    % (SHORTER, label))

    def test_every_shared_branch_still_agrees(self) -> None:
        """The control that makes the record above specific.

        If the copies differed somewhere else as well, the docstring's claim -- that the drift is
        one block -- would be wrong, and a test that only asserted a disagreement would not notice.
        """
        closure = _closure()
        longer = _compiled(COPIES[0][0], COPIES[0][1], closure)
        shorter = _compiled(COPIES[1][0], COPIES[1][1], closure)
        for label, record in SHARED_BRANCH_RECORDS.items():
            with self.subTest(branch=label):
                left = longer(dict(record))
                right = shorter(dict(record))
                self.assertEqual(
                    left, right,
                    "the copies now disagree on a branch they share (%s): %s against %s. The "
                    "record above says the drift is one block; it is not" % (label, left, right))
                self.assertTrue(
                    left, "the %s fixture resolved no scope through either copy, so it is not "
                          "reaching the branch it names" % label)

    def test_the_shorter_module_has_no_owner_map_at_all(self) -> None:
        """The mechanism, not just the branch.

        Asked of the SYNTAX rather than the text. A substring check passes while the map's
        declaration is renamed out from under the three lines that still mention it, which is a
        module that no longer builds the map and a test that says it does.
        """
        self.assertTrue(
            _binds(LONGER, OWNER_MAP),
            "%s no longer assigns %s, so the longer copy's extra block cannot be doing what this "
            "file says it does -- whatever still mentions the name is reading something nothing "
            "builds" % (LONGER, OWNER_MAP))
        self.assertFalse(
            _mentions(SHORTER, OWNER_MAP),
            "%s now names %s. The mechanism the shorter path lacked is being built; check whether "
            "the helper still diverges" % (SHORTER, OWNER_MAP))

    def test_only_one_module_builds_the_embedding_to_owner_direction(self) -> None:
        """The absence is a DIRECTION, not the whole mechanism.

        Both modules build the owner-to-scope map and consult it for an embedding. Only the longer
        one builds the embedding-to-scope map and consults it for an owner. If the shorter module
        ever binds the second, the record above is stale.
        """
        for stem in (LONGER, SHORTER):
            with self.subTest(module=stem, direction="owner to scope"):
                self.assertTrue(
                    _binds(stem, OTHER_DIRECTION_MAP),
                    "%s no longer builds %s, so the two modules no longer share the direction "
                    "this record says they share" % (stem, OTHER_DIRECTION_MAP))
        self.assertTrue(
            _binds(LONGER, OWNER_MAP),
            "%s no longer builds %s" % (LONGER, OWNER_MAP))
        self.assertFalse(
            _mentions(SHORTER, OWNER_MAP),
            "%s now names %s -- the missing direction is being built; check whether the helper "
            "still diverges" % (SHORTER, OWNER_MAP))

    def test_an_unrecovered_scope_is_dropped_not_kept(self) -> None:
        """The direction of the consequence, which the first version of this record left open.

        Every caller of the shorter copy spells its gate `if not recovered_scope_matches(...)`, so
        what an empty scope MEANS at that gate decides whether the divergence loses records or
        merely relabels them. It is a drop.
        """
        access_scope = _import("matrixark_mcp_access_scope")
        recovered = dict(SCOPE)
        for label, query in (
                ("prefer, the retrieval default",
                 dict(SCOPE, _session_scope="prefer",
                      _explicit_scope_keys=["tenant_id", "user_id"])),
                ("only", dict(SCOPE, _session_scope="only",
                              _explicit_scope_keys=["tenant_id", "user_id", "session_id"])),
                ("tenant only", {"tenant_hash": SCOPE["tenant_hash"]})):
            with self.subTest(query=label):
                self.assertTrue(
                    access_scope.scope_matches(dict(recovered), query),
                    "a RECOVERED scope no longer matches the %s query, so this fixture cannot "
                    "show what failing to recover costs" % label)
                self.assertFalse(
                    access_scope.scope_matches({}, query),
                    "an unrecovered scope now MATCHES the %s query. The divergence would then "
                    "keep records rather than drop them, which is the opposite consequence and "
                    "this record has to be rewritten" % label)

    def test_the_fixture_survives_the_other_import_spelling(self) -> None:
        """The regression that a same-process test cannot catch.

        This file passed on its own and failed under `unittest discover` for five branches,
        including one whose only change was a generated markdown document. The cause was here:
        `_closure` reached into `sys.modules` for the bare name of a module that, once something
        earlier in the run has imported `tools.matrixark_mcp_local_adapter`, exists only under its
        `tools.`-prefixed spelling. KeyError, in the two tests that build the closure.

        A test in this process cannot see that, because this process has whatever spelling it
        already has. So the scenario is run in a SUBPROCESS that imports the other spelling first.
        """
        import subprocess
        import textwrap

        repo = os.path.dirname(TOOLS)
        script = textwrap.dedent(
            """
            import sys
            sys.path.insert(0, %r)
            sys.path.insert(0, %r)
            import tools.matrixark_mcp_local_adapter          # the discovery-order import
            import tools.test_the_scope_recovery_helper_has_one_copy as guard
            closure = guard._closure()
            assert callable(closure["scope_from_node_path"]), "no scope_from_node_path"
            assert callable(closure["candidate_access_scope"]), "no candidate_access_scope"
            longer = guard._compiled(guard.COPIES[0][0], guard.COPIES[0][1], closure)
            record = {"record_type": "context_summary", "summary_hash": 9005}
            assert longer(dict(record)) == guard.SCOPE, "owner scope not recovered"
            print("OK")
            """
        ) % (repo, TOOLS)
        result = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True,
                                cwd=repo)
        self.assertEqual(
            0, result.returncode,
            "building the fixture fails when tools.matrixark_mcp_local_adapter is imported first, "
            "which is what a discovery run does. This file will pass alone and fail in the suite.\n"
            "%s" % (result.stderr.strip()[-800:] or result.stdout.strip()[-800:]))
        self.assertIn("OK", result.stdout, "the subprocess did not reach its assertions")

    def test_the_node_path_helper_has_exactly_one_definition(self) -> None:
        """It was defined twice, byte for byte, and now it is defined once.

        `matrixark_local_adapter_retrieve` publishes it at module scope; the retrieval module
        imports it rather than carrying a second copy. Asserted here because the guard next door
        matches on IDENTICAL bodies, so a copy that came back slightly changed -- the likely
        shape, since a copy that came back identical would have been edited for a reason -- would
        not register there at all.

        Both spellings of the import are required. One spelling is what broke this file across
        five branches; see the import-order test above.
        """
        definitions = [stem for stem in (LONGER, SHORTER)
                       if any(isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
                              and node.name == NODE_PATH_HELPER
                              for node in ast.walk(ast.parse(_source(stem))))]
        self.assertEqual(
            [SHORTER], definitions,
            "%s should be defined in %s alone and is defined in %s. If a second copy came back, "
            "the two will drift again -- that is what this pair did before"
            % (NODE_PATH_HELPER, SHORTER, ", ".join(definitions) or "nothing"))

        retrieval = _source(LONGER)
        for spelling in ("from tools.matrixark_local_adapter_retrieve import %s"
                         % NODE_PATH_HELPER,
                         "from matrixark_local_adapter_retrieve import %s" % NODE_PATH_HELPER):
            with self.subTest(spelling=spelling.split(" import ")[0]):
                self.assertIn(
                    spelling, retrieval,
                    "%s no longer reaches the published helper through this spelling. Both are "
                    "needed: which one resolves depends on how the process was started"
                    % LONGER)

    def test_the_older_nested_guard_structurally_cannot_see_this(self) -> None:
        """The reason this needed its own file, asserted instead of described.

        The older guard groups by the exact body text, so it reports the copies of this very pair
        that still AGREE and not the one that stopped. Imported HERE rather than at module level:
        under `unittest discover` a test module is reachable as both `tools.X` and bare `X`, so
        importing one test module from another at import time pulls a second copy into the run --
        see `test_matrixark_no_cross_test_imports`.
        """
        try:
            older = importlib.import_module("tools.test_a_nested_helper_has_one_copy_too")
        except ImportError:  # Direct script execution from tools/.
            older = importlib.import_module("test_a_nested_helper_has_one_copy_too")

        pairs = older.nested_duplicate_pairs()
        mine = frozenset({LONGER, SHORTER})
        self.assertIn(
            mine, pairs,
            "the older guard no longer groups anything for %s, so the comparison this test makes "
            "is vacuous" % " + ".join(sorted(mine)))
        names = pairs[mine]
        self.assertNotIn(
            HELPER, names,
            "the older guard now sees %s. If it grew drift matching, this record belongs there "
            "and this file should go" % HELPER)
        # The witness only has to be SOME still-identical helper in this module pair. It used to
        # be `scope_from_node_path`, which has since been consolidated to one implementation --
        # and this assertion failing is how that consolidation announced itself, which is the
        # behaviour wanted: a fix that removes the site a scan watches should not pass quietly.
        self.assertIn(
            "profile_summary_path_matches", names,
            "the older guard no longer reports an IDENTICAL helper in the same two functions, so "
            "it is not the sameness-keyed scan this file is contrasting itself with")


if __name__ == "__main__":
    unittest.main()
