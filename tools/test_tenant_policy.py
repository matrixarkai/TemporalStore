#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Per-tenant memory policy: resolution order, isolation between tenants, and live updates."""
from __future__ import annotations

import json
import os
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import matrixark_tenant_policy as policy
import matrixark_index_growth_bound as bound


class ResolutionOrderTest(unittest.TestCase):
    def setUp(self):
        policy.clear_tenant_policy_cache()
        self._env = {knob.env: os.environ.get(knob.env) for knob in policy.KNOBS.values()}
        self._path = os.environ.get("MATRIXARK_TENANT_POLICY_PATH")
        for knob in policy.KNOBS.values():
            os.environ.pop(knob.env, None)
        os.environ.pop("MATRIXARK_TENANT_POLICY_PATH", None)

    def tearDown(self):
        for name, value in self._env.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value
        if self._path is None:
            os.environ.pop("MATRIXARK_TENANT_POLICY_PATH", None)
        else:
            os.environ["MATRIXARK_TENANT_POLICY_PATH"] = self._path
        policy.clear_tenant_policy_cache()

    def test_builtin_defaults_when_nothing_is_configured(self):
        self.assertFalse(policy.resolve("extract_segments"))
        self.assertTrue(policy.resolve("generate_embeddings"))
        self.assertEqual(policy.resolve("max_secondary_index_records_per_scope"), 128)

    def test_env_overrides_default_and_tenant_overrides_env(self):
        os.environ["MATRIXARK_EXTRACT_SEGMENTS"] = "1"
        self.assertTrue(policy.resolve("extract_segments"), "env beats the built-in default")
        self.assertTrue(policy.resolve("extract_segments", {"tenant_id": "acme"}))
        policy.set_tenant_policy("acme", {"extract_segments": False})
        self.assertFalse(policy.resolve("extract_segments", {"tenant_id": "acme"}), "tenant beats env")
        self.assertTrue(policy.resolve("extract_segments", {"tenant_id": "other"}), "only that tenant moved")

    def test_policy_file_is_read_and_hot_reloaded(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "tenants.json"
            path.write_text(json.dumps({
                "defaults": {"max_secondary_index_records_per_scope": 512},
                "tenants": {"acme": {"extract_segments": True, "max_secondary_index_records_per_scope": 4096}},
            }), encoding="utf-8")
            os.environ["MATRIXARK_TENANT_POLICY_PATH"] = str(path)
            self.assertEqual(policy.resolve("max_secondary_index_records_per_scope", {"tenant_id": "acme"}), 4096)
            self.assertEqual(policy.resolve("max_secondary_index_records_per_scope", {"tenant_id": "b"}), 512,
                             "file defaults apply to tenants the file does not name")
            self.assertTrue(policy.resolve("extract_segments", {"tenant_id": "acme"}))
            # rewrite with a different mtime -- no restart, no cache clear
            os.utime(path, ns=(0, 0))
            path.write_text(json.dumps({"tenants": {"acme": {"max_secondary_index_records_per_scope": 32}}}),
                            encoding="utf-8")
            self.assertEqual(policy.resolve("max_secondary_index_records_per_scope", {"tenant_id": "acme"}), 32,
                             "an edited policy file must take effect without a restart")

    def test_a_broken_policy_file_keeps_the_last_good_policy(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "tenants.json"
            path.write_text(json.dumps({"tenants": {"acme": {"max_secondary_index_records_per_scope": 99}}}),
                            encoding="utf-8")
            os.environ["MATRIXARK_TENANT_POLICY_PATH"] = str(path)
            self.assertEqual(policy.resolve("max_secondary_index_records_per_scope", {"tenant_id": "acme"}), 99)
            os.utime(path, ns=(0, 0))
            path.write_text("{ this is not json", encoding="utf-8")
            self.assertEqual(policy.resolve("max_secondary_index_records_per_scope", {"tenant_id": "acme"}), 99,
                             "a bad edit must not silently reset every tenant to defaults")

    def test_unknown_knobs_and_bad_values_are_dropped_not_fatal(self):
        policy.set_tenant_policy("acme", {"not_a_knob": 1, "max_secondary_index_records_per_scope": "seventeen"})
        self.assertEqual(policy.resolve("max_secondary_index_records_per_scope", {"tenant_id": "acme"}), 128)

    def test_tenant_identity_from_every_scope_shape(self):
        self.assertEqual(policy.tenant_of({"tenant_id": "acme"}), "acme")
        self.assertEqual(policy.tenant_of({"tenant_hash": 42}), "42")
        self.assertEqual(policy.tenant_of("t=42;u=7"), "42")
        self.assertEqual(policy.tenant_of("acme"), "acme")
        self.assertEqual(policy.tenant_of(None), "")
        self.assertEqual(policy.tenant_of({}), "")

    def test_store_records_register_policy(self):
        found = policy.register_tenant_policy_records([
            policy.tenant_policy_record("acme", {"generate_embeddings": False}),
            {"record_type": "context_event", "event_id_hash": 1},
        ])
        self.assertEqual(found, 1)
        self.assertFalse(policy.resolve("generate_embeddings", {"tenant_id": "acme"}))


class PerTenantIndexBudgetTest(unittest.TestCase):
    """One tenant's cap must never spend another tenant's index budget."""

    def setUp(self):
        policy.clear_tenant_policy_cache()

    def tearDown(self):
        policy.clear_tenant_policy_cache()

    def _posting(self, tenant, ts):
        return {
            "record_type": "context_index",
            "ref_type": "event",
            "index_name": "keyword:x",
            "ref_hashes": [ts],
            "scope": {"tenant_id": tenant, "scope_key": f"t={tenant};u=1"},
            "scope_key": f"t={tenant};u=1",
            "timestamp_key_ms": ts,
            "index_hash": f"{tenant}-{ts}",
        }

    def test_each_tenant_is_capped_by_its_own_policy(self):
        policy.set_tenant_policy("big", {"max_secondary_index_records_per_scope": 0,
                                         "secondary_index_hard_ceiling": 0})
        policy.set_tenant_policy("small", {"max_secondary_index_records_per_scope": 2,
                                           "secondary_index_hard_ceiling": 0})
        records = [self._posting("big", ts) for ts in range(6)] + \
                  [self._posting("small", ts) for ts in range(6)]
        kept = bound.enforce_secondary_index_bounds(records)
        by_tenant = {}
        for record in kept:
            by_tenant.setdefault(record["scope"]["tenant_id"], []).append(record["timestamp_key_ms"])
        self.assertEqual(len(by_tenant["big"]), 6, "an uncapped tenant keeps everything")
        self.assertEqual(by_tenant["small"], [4, 5], "the capped tenant keeps only its newest")

    def test_a_busy_tenant_cannot_evict_a_quiet_tenants_index(self):
        # The ceiling is per tenant: 'noisy' blowing past it must not touch 'quiet'.
        policy.set_tenant_policy("noisy", {"max_secondary_index_records_per_scope": 0,
                                           "secondary_index_hard_ceiling": 3})
        policy.set_tenant_policy("quiet", {"max_secondary_index_records_per_scope": 0,
                                           "secondary_index_hard_ceiling": 3})
        records = [self._posting("noisy", ts) for ts in range(20)] + \
                  [self._posting("quiet", ts) for ts in range(2)]
        kept = bound.enforce_secondary_index_bounds(records)
        counts = {}
        for record in kept:
            tenant = record["scope"]["tenant_id"]
            counts[tenant] = counts.get(tenant, 0) + 1
        self.assertEqual(counts["noisy"], 3, "the noisy tenant is held to its own ceiling")
        self.assertEqual(counts["quiet"], 2, "the quiet tenant loses nothing to its neighbour")


class LiveTenantPolicyEndToEndTest(unittest.TestCase):
    """Two tenants in ONE store with different policies, then a policy change while it runs."""

    def setUp(self):
        policy.clear_tenant_policy_cache()

    def tearDown(self):
        policy.clear_tenant_policy_cache()

    def _counts(self, records, tenant):
        """Attribute rows to a tenant the way the store itself does.

        A served record often has no scope dict -- interning reduces it to a scope_key holding the
        tenant HASH -- so matching on tenant_id alone silently attributes nothing."""
        identities = {tenant, policy.tenant_hash_of(tenant)}
        out = {}
        for record in records:
            scope = record.get("scope") or record.get("access_scope") or record.get("scope_key")
            if policy.tenant_of(scope) not in identities:
                continue
            key = str(record.get("record_type") or "")
            out[key] = out.get(key, 0) + 1
        return out

    def _vector_rows(self, records, tenant):
        """How many of a tenant's rows actually CARRY a vector.

        Counting context_embedding rows stopped measuring this: append_many folds a vector onto
        the record that owns it and drops the separate row, so new logs hold none for anybody.
        That made "starter stores no vectors" true for every tenant, passing while proving
        nothing -- the assertion could no longer fail if the policy broke. A vector rides on its
        owner under `vector` or, once compacted, under `embedding_meta`.
        """
        identities = {tenant, policy.tenant_hash_of(tenant)}
        total = 0
        for record in records:
            scope = record.get("scope") or record.get("access_scope") or record.get("scope_key")
            if policy.tenant_of(scope) not in identities:
                continue
            if record.get("vector") or record.get("embedding_meta"):
                total += 1
        return total

    def test_two_tenants_one_store_get_different_storage(self):
        import matrixark_mcp_server as mcp

        policy.set_tenant_policy("enterprise", {"extract_segments": True, "generate_embeddings": True})
        policy.set_tenant_policy("starter", {"extract_segments": False, "generate_embeddings": False})
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            adapter = mcp.MatrixArkLocalAdapter(Path(tmp) / "memory.jsonl")
            server = mcp.MatrixArkMcpServer(adapter, access_mode="dev")

            def ingest(tenant, text):
                scope = {"tenant_id": tenant, "user_id": "u1", "session_id": "s1"}
                server.call_tool("matrixark_ingest", {"scope": scope, "finalize": True,
                                                      "messages": [{"role": "user", "content": text}]})
                server.call_tool("matrixark_session_commit", {"scope": scope})

            for turn in range(3):
                ingest("enterprise", f"turn {turn}: I am allergic to peanuts and I live in Kyoto.")
                ingest("starter", f"turn {turn}: I am allergic to peanuts and I live in Kyoto.")
            records = adapter.read_all()

            enterprise = self._counts(records, "enterprise")
            starter = self._counts(records, "starter")
            self.assertGreater(self._vector_rows(records, "enterprise"), 0, "enterprise keeps its vectors")
            self.assertEqual(self._vector_rows(records, "starter"), 0, "starter stores no vectors")
            self.assertGreater(enterprise.get("context_segment", 0), 0, "enterprise keeps segments")
            self.assertEqual(starter.get("context_segment", 0), 0, "starter stores no segments")
            self.assertGreater(starter.get("context_event", 0), 0, "starter still stores its memories")

            # ---- flip the small tenant's embeddings ON with the service running
            policy.set_tenant_policy("starter", {"generate_embeddings": True})
            ingest("starter", "turn 99: My favorite drink is matcha.")
            self.assertGreater(self._vector_rows(adapter.read_all(), "starter"), 0,
                               "a live policy change must take effect with no restart")


if __name__ == "__main__":
    unittest.main(verbosity=2)
