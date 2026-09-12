#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A definition only the tests call, inside a module production does reach.

Two guards sit either side of this and neither can see it.
`test_a_definition_nothing_reaches` asks whether ANYTHING names a definition, and a test naming it
counts -- so a function with a test and no caller reads as reached.
`test_a_module_only_tests_reach_is_not_live` asks the question at MODULE granularity, so a function
nothing calls inside a module the write path imports is invisible to it.

The gap between them is where a built feature goes to be forgotten. `backend_intern_records`,
`backend_expand_records` and `backend_intern_learn` implement backend metadata interning: a flag,
an encoder, a sidecar record type, a token table, a decoder, and tests for all of it. The two
production modules that import `matrixark_mcp_temporal_append` take `slim_persisted_record` and
nothing else. **The feature is complete and nothing invokes it.**

That is not an argument for deleting it -- see `test_no_module_is_orphaned_quietly` on reading the
orphan first, and the `ingest` cluster which is unwired rather than abandoned. It is an argument
for the condition being VISIBLE, because the failure it produces is a flag an operator can set that
changes nothing, and a test suite that stays green over code no request reaches.

WHAT IS RECORDED AND WHY THE GROUPS MATTER

Four kinds turned up, and they want different answers:

  * built and PARKED, with the blocking question written down -- the interning pair. Read the
    comment block above them before deciding anything: it names the measured prize (storage_options
    is 13.2% of all record bytes) and the reason the write side is off (the JSONL codec's
    crash-safety argument relies on an ordering the backend does not have). Retiring this deletes
    measured work whose open question is stated; wiring it needs that question answered first.
  * the unadopted part of a PARTLY-ADOPTED extraction -- `prepare_retrieval_request`,
    `retrieval_ranking_limits`, `prepare_serving_refs`, `merge_refreshed_summary_records`. All four
    modules are imported by production; these are the pieces the live path still does inline. This
    is the group to be most careful with, and my first note on them read "the live path does not
    call it", which invites exactly the wrong action. Read
    `test_a_tenant_override_reaches_the_extracted_ranking_limits` first: it calls its module "a
    partly-adopted extraction" and exists because the builder once resolved no tenant override at
    all, so adopting it would have handed every tenant the build default while looking like a pure
    code move.

    Adoption was priced rather than guessed at, for the ranking limits, which is the largest of the
    four. The builder and the live block agree on all NINE fields across 90 combinations of ranking
    dict and scope -- malformed values, a bad budget_fill_policy, a None scope, two tenants -- so
    equivalence is not the obstacle. What it costs is this:

      * `_tenant_retrieval_limit` stops being called by production and becomes a test-only
        definition. The count does not fall; the function moves INTO this list.
      * `test_matrixark_gateway_config_audit` reads the live module's SOURCE for
        `_tenant_retrieval_limit("name", ...)` call sites and asserts it covers five budgets. With
        the call sites gone it fails with "found no call sites; the scan is broken".
      * `test_a_tenant_override_reaches_the_extracted_ranking_limits` compares live call sites
        against extracted ones; with no live sites its floor of four fails.

    So adoption is a four-file change that relocates one function and blinds two guards that read
    the live call sites as their source of truth -- the shape
    `a-mechanical-rewrite-blinds-every-guard-that-reads-that-shape` warns about. Worth doing as its
    own piece of work, with those two guards redirected in the same commit. Not worth doing for a
    smaller function count, because it does not produce one.
  * a statistic nobody reports -- four `*_stats` functions that compute a number no surface prints.
  * a test affordance on purpose -- cache clearers and record builders that exist so a test can
    reset state. These are fine, and saying so is what stops the next sweep deleting them.

THE SCAN EXCLUDES ITSELF, and that is not optional

This file names every definition it decides about. Its own mentions land in the TEST bucket, so
without the exclusion a definition nothing calls would read as test-reached the moment it was
recorded here -- the same self-feeding that made the flag ratchet credit its own examples
(matrixarkai#1554), and the orphan guard before it.
"""
from __future__ import annotations

import ast
import collections
import io
import os
import re
import subprocess
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(TOOLS)
_SELF = os.path.join("tools", os.path.basename(__file__))

_WORD = re.compile(r"[A-Za-z_][A-Za-z_0-9]*")
_BINARY = (".png", ".jpg", ".jpeg", ".gif", ".ico", ".pdf", ".zip", ".bin", ".so", ".whl")
_MAX_BYTES = 4_000_000

#: A floor on the corpus. A scan that read nothing reports a clean tree.
EXPECTED_FILE_FLOOR = 400

#: definition -> what it is. Asserted EXACTLY: a new one fails here rather than accumulating, and
#: one that gains a production caller fails too, because a list allowed to go stale describes a
#: tree that no longer exists.
ONLY_TESTS_CALL = {
    "backend_intern_records":
        "backend metadata interning, the encoder. PARKED ON PURPOSE, not forgotten: its own comment block says storage_options is the largest field in the store -- 2,139 KB, 13.2% of all record bytes, nine distinct values across 3,610 rows -- and that INTERN_METADATA_FIELDS already names it while the local JSONL codec already tokenises it, so the optimisation landed on the 7-day mirror and not the durable store. The write side is gated OFF because the codec's crash-safety argument does not carry over: the backend would rely on the engine's batch append being atomic, which that comment calls a different claim and unverified, ending 'Do not flip this default until that is settled.' Retiring it deletes measured work with a written open question",
    "backend_expand_records":
        "backend metadata interning, the decoder. Always on by design -- a store written with the flag ON still reads correctly if it is later turned OFF, the same asymmetry the JSONL codec uses",
    # ADOPTED AND BANKED, so they are no longer on this list:
    #
    #   full_read_fallback_counts -- matrixarkai#1566 gave it a Prometheus family in
    #   matrixark_temporal_direct_backend, so the degradation it measures is now reported.
    #   embedding_cache_stats -- matrixarkai#1569 put it on the local adapter dashboard.
    #   pipeline_task_footprint_stats -- matrixarkai#1570 put rows against distinct tasks on
    #   the same dashboard. Third of three, and the third to leave within days of being
    #   written down, which is the list working rather than the list being wrong.
    #
    # Both entries said the same thing in different words, "written to make something visible and
    # reported nowhere", and both stopped being true within a week of being written down. That is
    # what this list is FOR: it is not an inventory of debris, it is a queue, and an entry leaving
    # it by gaining a production caller is the outcome. Removing them here is the banking -- the
    # check fails on a list that has stopped matching the tree in EITHER direction, which is how
    # this pair was noticed at all.
    "prepare_retrieval_request":
        "the unadopted part of a PARTLY-ADOPTED extraction. matrixark_mcp_retrieve_request is imported by five production modules; this step is the piece LocalAdapter.retrieve still does inline",
    "prepare_serving_refs":
        "the unadopted part of matrixark_mcp_retrieve_pack_builder, which three production modules import. The live retrieve calls both functions it wraps, back to back, at matrixark_local_adapter_retrieve:3678",
    "retrieval_ranking_limits":
        "the unadopted part of matrixark_mcp_retrieve_planning, and the one with a guard already maintaining it TOWARD adoption. test_a_tenant_override_reaches_the_extracted_ranking_limits calls that module 'a partly-adopted extraction' and exists because the builder once resolved no tenant override at all, so adopting it would have handed every tenant the build default while looking like a pure code move. Do not read this as debris",
    "merge_refreshed_summary_records":
        "the unadopted part of matrixark_mcp_retrieve_pre_refresh, which three production modules import. The same merge runs inline at matrixark_local_adapter_retrieve:1167-1196",
    "secondary_index_bound_stats":
        "live posting counts by scope and ref_type. Its own docstring says 'used by the tests/harness', so this one is an affordance by design rather than a signal that went missing -- read the docstring before filing it as a gap",
    "env_int":
        "the typed integer env reader. env_bool has twenty-two production callers and this has none, so most integer flags are parsed at their own read site instead",
    "env_float":
        "the typed float env reader, unused for the same reason as env_int beside it",
    "contract_from_values":
        "builds the shared OSS model contract from values; only its own test consults it",
    "validate_shared_oss_contract":
        "validates that contract; the validate_ gate scripts do not call it",
    "default_judge":
        "the default benchmark judge; the harness picks its judge another way",
    "node_summary_plan":
        "plans which node summaries to write; no caller on the write path",
    "path_items":
        "splits an ingestion job path into items; no caller on the ingest path",
    "retrieval_records_cache_key":
        "builds a cache key for retrieval records; nothing caches them by it",
    "skill_ingest_envelope":
        "builds the kind='skill' ingest envelope and its docstring says it is 'for the live import path (ingest_resource_or_skill_if_needed)'. That function IS live, called from matrixark_mcp_local_ingest -- with the envelope the REQUEST carried. 'kind': 'skill' appears in exactly one non-test place, inside this function, so nothing bridges skill DISCOVERY to ingestion: a caller has to send the envelope itself",
    "clear_tenant_policy_cache":
        "a test affordance: resets the tenant policy cache between cases, so a test does not inherit the previous one's policy",
    "_reset_live_cache":
        "a test affordance: resets the v1 gateway live cache between cases",
    "tenant_policy_record":
        "builds a tenant policy record for a test to store; the live path writes its own shape",
    "user_policy_record":
        "the symmetric half of the record builder above, and half a symmetric pair is worse than both",
}


#: Test files that are a REGISTRY of definition names rather than a caller of them. Named rather
#: than derived, and asserted exactly by a test below, because the derived version was worse in
#: both directions: "any ALL-CAPS collection of identifier-shaped strings" matched ordinary tests
#: writing a tuple of field names, and tightening it to "strings that name real definitions" still
#: matched six settings tests whose key lists happen to collide with function names. Excluding a
#: real test makes the feature it covers read as called by nothing, which is the failure this file
#: exists to report.
#:
#: Two files qualify. `test_a_definition_nothing_reaches` lists the definitions NOTHING reaches --
#: naming them is its whole job, and counting that as a call would credit every entry on it. This
#: file lists the definitions only tests call, for the same reason.
#:
#: `test_a_module_only_tests_reach_is_not_live` is NOT here: it lists module stems, which are not
#: definition names, so it credits nothing.
NAME_REGISTRIES = (
    "tools/test_a_definition_nothing_reaches.py",
    "tools/test_a_definition_only_the_tests_call.py",
)


def _tracked_text():
    listed = subprocess.run(["git", "ls-files"], cwd=REPO,
                            capture_output=True, text=True).stdout.split()
    out = {}
    for rel in listed:
        if os.path.splitext(rel)[1].lower() in _BINARY:
            continue
        path = os.path.join(REPO, rel)
        try:
            if os.path.getsize(path) > _MAX_BYTES:
                continue
            with io.open(path, encoding="utf-8", errors="replace") as handle:
                out[rel] = handle.read()
        except OSError:
            continue
    return out


def _is_test(rel):
    return os.path.basename(rel).startswith("test_")


def only_tests_call():
    """{definition: module stem} for top-level defs in live modules that only tests name.

    Counting every TRACKED file, not only `tools/*.py`. The first version of this read Python
    alone and reported `decode_payload`, `extract_prompt` and `extract_identity` as test-only --
    all three are imported by Python embedded in `matrixark_codex_dual_hook.sh`, and
    `http_portal_main` is a console script named in pyproject.toml. A caller does not stop being
    one for living in a shell script.
    """
    texts = _tracked_text()
    tokens = {rel: collections.Counter(_WORD.findall(text)) for rel, text in texts.items()}

    inventories = sorted(NAME_REGISTRIES)
    non_test = collections.Counter()
    by_tests = collections.Counter()
    for rel, counted in tokens.items():
        if _is_test(rel):
            if rel not in NAME_REGISTRIES:
                by_tests.update(counted)
        else:
            non_test.update(counted)

    recorded_modules = set(re.findall(
        r'"(matrixark_[a-z0-9_]+)"',
        texts.get("tools/test_a_module_only_tests_reach_is_not_live.py", "")))

    found = {}
    for rel, text in texts.items():
        if not rel.startswith("tools/") or not rel.endswith(".py") or _is_test(rel):
            continue
        stem = os.path.basename(rel)[:-3]
        if stem in recorded_modules:
            continue
        try:
            tree = ast.parse(text)
        except SyntaxError:
            continue
        for node in tree.body:
            if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            name = node.name
            if name.startswith("__") or name == "main":
                continue
            own = tokens[rel][name]
            if own == 1 and non_test[name] - own == 0 and by_tests[name] > 0:
                found[name] = stem
    return found, len(texts), sorted(inventories)


class ADefinitionOnlyTheTestsCallTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.found, cls.files, cls.inventories = only_tests_call()

    def test_only_the_two_registries_are_excluded(self) -> None:
        """Print the denominator. An exclusion wide enough to cover ordinary tests makes this
        report a clean tree by excluding the callers.

        Both derived versions were tried and both were worse. "Any ALL-CAPS collection of
        identifier-shaped strings" matched a test writing a tuple of field names. Tightening it to
        "strings that name real definitions" still matched six settings tests whose key lists
        collide with function names -- and excluding a real test makes the feature it covers read
        as called by nothing.
        """
        self.assertEqual(sorted(NAME_REGISTRIES), self.inventories)
        for rel in NAME_REGISTRIES:
            with self.subTest(registry=rel):
                self.assertTrue(
                    os.path.exists(os.path.join(REPO, rel)),
                    "%s is excluded as a registry and does not exist, so the exclusion is "
                    "silently covering nothing" % rel)

    def test_the_scan_reads_the_tree(self) -> None:
        self.assertGreaterEqual(
            self.files, EXPECTED_FILE_FLOOR,
            "only %d tracked files were read, so the check below is about almost nothing"
            % self.files)

    def test_the_scan_still_finds_something(self) -> None:
        """A positive control. Empty is also what a scan that stopped parsing prints."""
        self.assertTrue(
            self.found,
            "no definition anywhere is called only by tests. That would be good news and it is "
            "also what a broken scan says, so check the scan before believing it.")

    def test_the_set_is_exactly_what_is_recorded(self) -> None:
        self.assertEqual(
            sorted(ONLY_TESTS_CALL), sorted(self.found),
            "a definition is called only by tests and is not recorded, or one on the list has "
            "gained a production caller. Either way the tree moved and the list did not.")

    def test_every_entry_says_what_it_is(self) -> None:
        thin = sorted(name for name, note in ONLY_TESTS_CALL.items() if len(note.strip()) < 24)
        self.assertEqual([], thin, "recorded with nothing recorded: %s" % thin)

    def test_the_scan_does_not_feed_on_this_file(self) -> None:
        """Every name here is written here. Counting this file's mentions as a test call would
        make a definition look test-reached the moment it was recorded -- the self-feeding that
        made the flag ratchet credit its own examples, and the orphan guard before it."""
        texts = _tracked_text()
        own = set(_WORD.findall(texts.get(_SELF, "")))
        self.assertTrue(
            set(ONLY_TESTS_CALL) & own,
            "this file names none of the definitions it records, so either the record emptied or "
            "the read failed -- and the exclusion is then hiding nothing")
        for name in ONLY_TESTS_CALL:
            with self.subTest(definition=name):
                elsewhere = any(name in text for rel, text in texts.items()
                                if _is_test(rel) and rel != _SELF)
                self.assertTrue(
                    elsewhere,
                    "%s is recorded as called by tests and the only test naming it is this one. "
                    "The exclusion is not working." % name)


if __name__ == "__main__":
    unittest.main()
