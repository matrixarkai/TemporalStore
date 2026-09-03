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
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

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


if __name__ == "__main__":
    unittest.main()
