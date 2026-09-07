# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""One adapter serving two tenants must not answer one of them with the other's pack.

`canonical_scope_key` returns "" for a scope carrying neither `scope_key` nor `tenant_hash` --
which is exactly the shape of the documented public scope, {tenant_id, user_id, session_id}. Both
the retrieval-records cache and the context-pack cache keyed on it, so with a raw scope every
tenant shared one entry and the second to ask a question was served the first one's answer.

Reproduced on one adapter with two tenants: tenant B asked what its own pet was called and was
given tenant A's. Clearing EITHER cache alone did not help, because both keys collapsed the same
way -- which is what made it look like a selection bug rather than a key bug.

The MCP entry point normalises the scope before this point, so the served paths were not affected.
That is not something to rely on: nothing at the cache said so, and a caller skipping
normalisation got cross-tenant answers with no error to notice.
"""
import ast
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
TOOLS_DIR = os.path.dirname(os.path.abspath(__file__))

import matrixark_mcp_local_adapter as adapter_module
import matrixark_mcp_core_identity as core_identity
import matrixark_mcp_identity as identity

ACME = {"tenant_id": "acme", "user_id": "dana", "session_id": "s1"}
GLOBEX = {"tenant_id": "globex", "user_id": "rui", "session_id": "s1"}
QUERY = "what is my pet called"


def _seed(log):
    adapter = adapter_module.MatrixArkLocalAdapter(log)
    for scope, turns in (
        (ACME, [("user", "My dog is called Mochi."), ("assistant", "Mochi noted.")]),
        (GLOBEX, [("user", "My cat is called Pixel."), ("assistant", "Pixel noted.")]),
    ):
        for role, content in turns:
            adapter.ingest({"kind": "message", "scope": scope,
                            "messages": [{"role": role, "content": content}], "finalize": True})
    for scope in (ACME, GLOBEX):
        for name, args in (("session_commit", {"scope": scope}),
                           ("refresh_summaries", {"scope": scope, "limit": 20})):
            try:
                getattr(adapter, name)(args)
            except Exception:
                pass
    return adapter


def _served(adapter, scope):
    pack = adapter.retrieve({"scope": scope, "query": QUERY})
    return json.dumps(pack.get("selected_refs") or [], default=str)


class CacheKeySeparatesTenants(unittest.TestCase):
    def setUp(self):
        with adapter_module._LOCAL_READ_CACHE_LOCK:
            adapter_module._LOCAL_READ_CACHE.clear()

    def test_the_second_tenant_is_not_served_the_first_ones_pack(self):
        log = Path(tempfile.mkdtemp()) / "events.jsonl"
        _seed(log)
        adapter = adapter_module.MatrixArkLocalAdapter(log)

        first = _served(adapter, ACME)
        self.assertIn("Mochi", first, "the first tenant did not get its own content, so the "
                                      "second getting it would prove nothing")
        second = _served(adapter, GLOBEX)
        self.assertNotIn("Mochi", second, "the second tenant was served the first tenant's content")
        self.assertIn("Pixel", second, "the second tenant was not served its own content")

    def test_a_raw_scope_still_produces_a_distinguishing_cache_key(self):
        """The mechanism, stated directly: canonical_scope_key collapses, the cache key must not."""
        for module in (core_identity, identity):
            self.assertEqual("", module.canonical_scope_key(ACME),
                             "%s: a raw scope is expected to have no canonical key" % module.__name__)
            self.assertEqual("", module.canonical_scope_key(GLOBEX))
            self.assertNotEqual(
                module.cache_scope_key(ACME), module.cache_scope_key(GLOBEX),
                "%s: two tenants share one cache key" % module.__name__)

    def test_a_normalised_scope_still_uses_its_canonical_key(self):
        """The fallback must not take over when there IS a canonical key to use."""
        for module in (core_identity, identity):
            normalised = {"scope_key": "tenant|user|session"}
            self.assertEqual(("k", "tenant|user|session"), module.cache_scope_key(normalised))
            self.assertNotEqual(module.cache_scope_key(normalised), module.cache_scope_key(ACME))

    def test_both_copies_of_the_helper_agree(self):
        """There are two identity modules and the live path resolves core_identity. A behavioural
        check rather than a shared symbol, which is how the twins are already treated here."""
        for scope in (ACME, GLOBEX, {"scope_key": "x"}, {}):
            self.assertEqual(core_identity.cache_scope_key(scope), identity.cache_scope_key(scope),
                             "the two copies disagree for %r" % (scope,))


class EveryCacheKeyBuilderSeparatesTenants(unittest.TestCase):
    """The fix above went in where the incident was found: the live retrieve path, which builds its
    key inline, and the two identity helpers. It did not reach the EXTRACTED builders, which exist
    so that inline code can be replaced by them -- so adopting either extraction would have put the
    incident straight back. `context_pack_cache_key` and `retrieval_records_cache_key` are the two
    caches the module docstring above names, in their extracted form.

    Derived from the source rather than from a list of the two, because a list cannot see the copy
    somebody extracts next month -- which is precisely how these two came to be missed.
    """

    #: A builder keyed on a scope must not key on the function that returns "" for a raw scope.
    COLLAPSING = "canonical_scope_key"
    SEPARATING = "cache_scope_key"

    @staticmethod
    def _builders():
        """(module file, function name, names it calls) for every cache-key builder in tools/."""
        found = []
        for entry in sorted(os.listdir(TOOLS_DIR)):
            if not entry.endswith(".py") or entry.startswith("test_"):
                continue
            try:
                tree = ast.parse(Path(TOOLS_DIR, entry).read_text(encoding="utf-8"))
            except (SyntaxError, UnicodeDecodeError):
                continue
            for node in ast.walk(tree):
                if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    continue
                if "cache_key" not in node.name:
                    continue
                calls = set()
                for sub in ast.walk(node):
                    if isinstance(sub, ast.Call):
                        if isinstance(sub.func, ast.Name):
                            calls.add(sub.func.id)
                        elif isinstance(sub.func, ast.Attribute):
                            calls.add(sub.func.attr)
                found.append((entry, node.name, calls))
        return found

    def test_the_scan_finds_the_builders(self):
        """Without this, a scan that matched nothing would report every builder clean."""
        builders = self._builders()
        self.assertGreaterEqual(
            len(builders), 10,
            "found %d cache-key builders under tools/, expected at least 10 -- the scan stopped "
            "matching, so the check below proves nothing" % len(builders))
        self.assertIn(
            ("matrixark_mcp_retrieve_cache.py", "context_pack_cache_key"),
            [(f, n) for f, n, _c in builders],
            "the builder this guard was written for is no longer being scanned")

    def test_no_builder_keys_on_the_collapsing_scope_key(self):
        offenders = [
            "%s:%s" % (f, n) for f, n, calls in self._builders()
            if self.COLLAPSING in calls and self.SEPARATING not in calls
        ]
        self.assertEqual(
            [], offenders,
            "these cache-key builders key on %s, which is \"\" for the documented public scope "
            "shape, so every tenant shares one entry: %s" % (self.COLLAPSING, ", ".join(offenders)))

    def test_the_extracted_builders_give_two_tenants_two_keys(self):
        """The behaviour, not the spelling -- a builder could collapse some other way."""
        import matrixark_mcp_retrieval_records as records
        import matrixark_mcp_retrieve_cache as pack_cache

        keys = []
        for scope in (ACME, GLOBEX):
            keys.append(records.retrieval_records_cache_key(
                generation=1, scope=scope, allowed_types={"context_event"}))
        self.assertNotEqual(keys[0], keys[1],
                            "retrieval_records_cache_key gives two tenants one key")

        class _Target:
            _retrieval_records_cache_generation = 1

        keys = []
        for scope in (ACME, GLOBEX):
            keys.append(pack_cache.context_pack_cache_key(
                _Target(), scope=scope, query=QUERY, question_type="general",
                retrieval_session_scope="session", max_context_tokens=1000,
                local_budget={"token_estimate": 0, "text_hashes": set()},
                ranking={}, include_superseded=False))
        self.assertNotEqual(keys[0], keys[1],
                            "context_pack_cache_key gives two tenants one key")


if __name__ == "__main__":
    unittest.main()
