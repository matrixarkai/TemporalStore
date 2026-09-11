#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A definition nothing reaches, inside a module everything does.

Two checks next door ask about MODULES: `test_no_module_is_orphaned_quietly` asks whether a
module's name appears anywhere, and `test_a_module_only_tests_reach_is_not_live` asks whether a
production entry point can get to it. Both pass for a live module that happens to carry a function
nobody calls, and that is the shape this file is about -- `matrixark_mcp_storage_options` is a
LIVE_ROOT and three of its nine definitions are reached by nothing at all.

It matters for the same reason the module checks do: an implementation that cannot run cannot be
wrong today, and is exactly what somebody reaches for tomorrow. `normalize_record_storage_options`
below is 46 lines that validate a `record_storage_options` request field, raising three different
errors for three malformed shapes -- and no caller passes it, so the field is accepted nowhere.
Reading it, you would think the feature ships.

HOW IT DECIDES
--------------
1. A name is an ENTRY POINT if any OTHER tracked file contains it as an identifier -- any
   extension. A launcher shell script with an inline python snippet is a real caller, and a scan
   that reads only `*.py` calls `matrixark_codex_hook_payload` dead because its three consumers
   are a `.sh` and a test.
2. Module-level code counts as an entry point for what it names, and a module with a `__main__`
   guard seeds from `main`.
3. From there, follow every bare name and attribute leaf back into the module's own top level,
   repeatedly, until the set stops growing. Without that closure a helper whose only caller sits
   in the same file reads as dead -- the intra-module false positive that once turned 1,615
   "unused" functions into 37 real ones.

An underscore prefix is a CONVENTION, not a scope. Seeding entry points from public names only
reported `_LocalAdapterRetrievalMixin` -- imported by name in `matrixark_mcp_local_adapter` -- as
846 dead lines. `test_the_control_name_reads_as_reached` pins that case.

TESTS COUNT AS CALLERS, deliberately. A definition only its own test calls is not reported here:
at module granularity that question belongs to the file next door, and at definition granularity
it would flood this list with fixtures and leave the real ones unreadable.

WHY THIS FILE IS EXCLUDED FROM ITS OWN CORPUS
---------------------------------------------
Every name below appears in this file, so a scan that read it would find each one "used
elsewhere" and report nothing -- a guard feeding on its own list. The exclusion is not a detail;
`test_the_guard_does_not_feed_on_its_own_list` asserts that dropping it empties the result.
"""
from __future__ import annotations

import ast
import io
import os
import re
import sys
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(TOOLS)
_SELF = os.path.abspath(__file__)
_WORD = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
_SKIP_DIRS = {".git", "__pycache__", "node_modules", "target"}
_MAX_BYTES = 4_000_000

sys.path.insert(0, TOOLS)


def _already_recorded() -> set:
    """Modules whose unreachability is already a recorded decision next door.

    Re-listing their definitions here would say the same thing twice and make both lists harder
    to keep true. Imported HERE rather than at module level: under `unittest discover` a test
    module is reachable as both `tools.X` and bare `X`, so importing one test module from another
    at import time pulls a second copy into the run and shifts what every later module sees --
    see `test_matrixark_no_cross_test_imports`, which exists because that cost an afternoon.
    """
    from test_a_module_only_tests_reach_is_not_live import UNREACHABLE

    return {name for names in UNREACHABLE.values() for name in names}

#: Reached by nothing, anywhere -- not by another module, not by a launcher, not by a test.
#: Asserted EXACTLY: a new one fails here rather than accumulating, and one that becomes reached
#: fails too, because a list allowed to go stale describes a tree that no longer exists.
UNREACHED = {
    # A request-level storage API that is accepted nowhere. `normalize_record_storage_options`
    # validates a `record_storage_options` object keyed by record kind and nothing calls it;
    # `normalize_part_storage_options` is a one-line alias of it, and `storage_part_for_record` a
    # one-line alias of `storage_record_kind`. The ENVELOPE path for the same idea does work --
    # `storage_options_for_record` reads `envelope["record_storage_options"]` -- so this is a
    # half-wired feature rather than a dead one, and which half should go is a product call.
    "matrixark_mcp_storage_options.py": ("normalize_record_storage_options",),
    # Two spellings of "the commonest memory layer among these refs", in two modules that both
    # also define `serving_ref_for_pack`. Neither is called.
    "matrixark_mcp_core_packing.py": ("default_memory_layer_for_pack",),
    "matrixark_mcp_context_pack.py": ("_default_memory_layer_for_pack",),
    # These are not decayed code. `git log -S` on each name shows almost every one arriving in a
    # SINGLE commit and never being touched again -- bulk publish and port commits (4379a739d
    # "publish the tooling behind index growth, task slimming and tenant policy", 1886b7056,
    # 06a46d19f, d422a7e21 "OSS sync PR 4/4"), module-split refactors (efd36f9ef, 667b9e17a,
    # 7c80869cc), or a feature PR whose caller never followed. Import residue, so "left behind by
    # the path that used to call it" is the wrong story: nothing ever called them here.
    # Two whose docstrings say where they are applied, and neither is applied anywhere:
    # `clip_messages_for_ingest` says "Applied ONCE at the ingest boundary", and
    # `joined_summary_source_text` is the sentence-level summary dedup, measurement included.
    "matrixark_index_growth_bound.py": ("clip_messages_for_ingest", "joined_summary_source_text"),
    "matrixark_load_config.py": ("apply_from_file",),
    # `clear_user_policy_cache` stays: it is the twin of `clear_tenant_policy_cache`, which is
    # itself called only from tests. Deleting one half of a symmetric test affordance makes the
    # module worse rather than smaller.
    "matrixark_tenant_policy.py": ("clear_user_policy_cache",),
    # `engine_blob_sweep` is not one more unreached helper. It is the engine attachment tier's
    # ONLY deletion path -- for unreferenced blobs AND for stale staging files -- and in
    # engine/resource_blobs.rs it is reachable from exactly one place, the
    # Command::ContextResourceBlobSweep arm, so nothing collects unless something asks. Nothing
    # asks: the live adapter exposes resource_blob_sweep, this wrapper is its only caller, and
    # this wrapper has none. TemporalStoreBlobClient, the other tier, has put/get/exists and no
    # delete at all. Do not wire a sweep to clear this entry: the caller must enumerate every
    # manifest still naming a content hash for that tenant, there is no such enumerator, and a
    # wrong referenced set deletes live attachments.
    "matrixark_temporalstore_blob.py": ("BlobPutResult", "engine_blob_sweep"),
    # Reporting scripts: these are the rows and lookups their own main stopped printing.
    "run_matrixark_message_pdf_debug_trace.py": (
        "embedding_model_name_for_display",
        "event_display_rows",
        "latest_records_by_key",
        "model_registry_map",
    ),
    # `require_int_between` stays for the same reason as the cache clearer above: its neighbour
    # `require_string_set` is used, and half a validation vocabulary is worse than all of it.
    "validate_context_resource_skill_scale.py": ("require_int_between",),
}


def _corpus(*, include_self: bool):
    """Every tracked file as a set of identifiers, keyed by path."""
    texts: dict = {}
    tokens: dict = {}
    for root, dirs, names in os.walk(REPO):
        dirs[:] = [d for d in dirs if d not in _SKIP_DIRS]
        for name in names:
            path = os.path.join(root, name)
            if not include_self and os.path.abspath(path) == _SELF:
                continue
            try:
                if os.path.getsize(path) > _MAX_BYTES:
                    continue
                with io.open(path, encoding="utf-8", errors="replace") as handle:
                    text = handle.read()
            except OSError:
                continue
            texts[path] = text
            tokens[path] = set(_WORD.findall(text))
    return texts, tokens


def _names_used(node) -> set:
    used = set()
    for sub in ast.walk(node):
        if isinstance(sub, ast.Name):
            used.add(sub.id)
        elif isinstance(sub, ast.Attribute):
            used.add(sub.attr)
    return used


def _unreached_in(module_file: str, texts, tokens):
    src = os.path.join(TOOLS, module_file)
    text = texts.get(src)
    if text is None:
        return None
    try:
        tree = ast.parse(text)
    except SyntaxError:
        return None
    tops = {
        node.name: node
        for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))
    }
    if not tops:
        return None
    top_names = set(tops)

    reached = set()
    for path, identifiers in tokens.items():
        if path != src:
            reached |= identifiers & top_names
    if "__main__" in text and "main" in tops:
        reached.add("main")
    for node in tree.body:
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            reached |= _names_used(node) & top_names

    changed = True
    while changed:
        changed = False
        for name in list(reached):
            node = tops.get(name)
            if node is None:
                continue
            for used in _names_used(node):
                if used in tops and used not in reached:
                    reached.add(used)
                    changed = True

    return tops, sorted(top_names - reached)


def _scan(*, include_self: bool = False):
    texts, tokens = _corpus(include_self=include_self)
    already_recorded = _already_recorded()
    found: dict = {}
    examined = 0
    for module_file in sorted(f for f in os.listdir(TOOLS) if f.endswith(".py")):
        if module_file.startswith("test_") or module_file[:-3] in already_recorded:
            continue
        result = _unreached_in(module_file, texts, tokens)
        if result is None:
            continue
        examined += 1
        _, unreached = result
        if unreached:
            found[module_file] = tuple(unreached)
    return found, examined, len(texts)


class ADefinitionNothingReachesTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.found, cls.examined, cls.files = _scan()

    def test_the_control_name_reads_as_reached(self):
        """A mixin imported BY NAME from another module must not read as unreached.

        `matrixark_mcp_local_adapter` does `from matrixark_local_adapter_retrieval import
        _LocalAdapterRetrievalMixin`. Seeding entry points from public names only made this scan
        report it, and its 846 lines, as dead. Nothing else in this file would have noticed.
        """
        self.assertNotIn("_LocalAdapterRetrievalMixin",
                         self.found.get("matrixark_local_adapter_retrieval.py", ()))
        self.assertNotIn("_LocalAdapterRetrieveMixin",
                         self.found.get("matrixark_local_adapter_retrieve.py", ()))

    def test_the_scan_reaches_the_tree(self):
        """Zero findings is also what a scan that read nothing prints."""
        self.assertGreaterEqual(self.files, 1000,
                                "only %d files read; the walk is not seeing the tree" % self.files)
        self.assertGreaterEqual(self.examined, 200,
                                "only %d modules examined" % self.examined)
        self.assertTrue(_already_recorded(),
                        "the registry exclusion came back empty, which silently widens this scan")

    def test_the_scan_still_finds_something(self):
        """A positive control: the method must still be able to report a definition.

        Named rather than counted, so a scan that started reporting a different set does not pass
        this by arithmetic. `normalize_record_storage_options` is the one checked by hand -- its
        only occurrence in the tree is its own `def` line.
        """
        self.assertIn("normalize_record_storage_options",
                      self.found.get("matrixark_mcp_storage_options.py", ()),
                      "the scan no longer finds a definition verified by hand to have no caller")

    def test_the_guard_does_not_feed_on_its_own_list(self):
        """Every name recorded here appears HERE, so reading this file would clear the list.

        Run with the exclusion dropped, the scan must come back empty. If it does not, the
        exclusion has stopped doing anything, and the exact-set test below has stopped meaning
        anything with it.
        """
        found, _, _ = _scan(include_self=True)
        self.assertEqual(
            {}, found,
            "this file names every recorded definition, so a scan that reads it should find "
            "none -- these survived, which means the self-exclusion is no longer load-bearing "
            "and neither is this guard: %s" % sorted(found))

    def test_the_set_is_exactly_what_is_recorded(self):
        self.assertEqual(
            {key: tuple(value) for key, value in sorted(UNREACHED.items())},
            {key: tuple(value) for key, value in sorted(self.found.items())},
            "a definition reached by nothing was added, or one on the list is reached now. "
            "Adding a name here is a decision to keep code that does not run -- prefer deleting "
            "it, and read it first: the orphan is sometimes the more complete copy.")


if __name__ == "__main__":
    unittest.main()
