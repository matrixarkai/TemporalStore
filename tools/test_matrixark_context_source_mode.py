#!/usr/bin/env python3
"""Context-source policy: synthetic/debug -> remote-only, real sessions -> local+remote.

Covers resolve_context_source_mode (the routing + the future global flip) and the
synthetic classifier that the hook uses to populate the request flag.
"""
import os
import subprocess
import sys
import unittest

import matrixark_mcp_runtime_config as cfg
from matrixark_mcp_runtime_config import (
    apply_remote_only_local_fallback,
    resolve_context_source_mode,
)
from matrixark_codex_hook import is_synthetic_hook_text


def _remote_only_budget(local_items, observed_tokens):
    """Mirror what the retrieve path stashes when it resolves a request to remote_only."""
    return {
        "context_source_mode": "remote_only",
        "items": [],
        "token_estimate": 0,
        "_remote_only_fallback_items": list(local_items),
        "observed_local_token_estimate": observed_tokens,
    }


class ContextSourceModeTest(unittest.TestCase):
    def test_synthetic_request_is_remote_only(self):
        self.assertEqual("remote_only", resolve_context_source_mode({"synthetic": True}))

    def test_real_request_keeps_local_and_remote(self):
        self.assertEqual("local_and_remote", resolve_context_source_mode({"synthetic": False}))
        self.assertEqual("local_and_remote", resolve_context_source_mode({}))
        self.assertEqual("local_and_remote", resolve_context_source_mode(None))

    def test_explicit_per_request_override_wins_over_synthetic(self):
        # A real prompt can be forced remote-only, and a synthetic one forced legacy.
        self.assertEqual(
            "remote_only",
            resolve_context_source_mode({"context_source_mode": "remote_only", "synthetic": False}),
        )
        self.assertEqual(
            "local_and_remote",
            resolve_context_source_mode({"context_source_mode": "local_and_remote", "synthetic": True}),
        )

    def test_global_flip_via_default_mode_forces_remote_only_for_everything(self):
        # Simulates the future "TemporalStore is good enough -> flip everything" switch.
        self.assertEqual(
            "remote_only",
            resolve_context_source_mode({"synthetic": False}, default_mode="remote_only"),
        )

    def test_default_mode_auto_is_the_shipped_default(self):
        self.assertEqual("auto", cfg.DEFAULT_CONTEXT_SOURCE_MODE)

    def test_env_global_flip_is_honored_at_import(self):
        # The env var is read at import time; prove the flip in a fresh interpreter.
        code = (
            "import sys; sys.path.insert(0, 'tools');"
            "import matrixark_mcp_runtime_config as c;"
            "print(c.DEFAULT_CONTEXT_SOURCE_MODE, c.resolve_context_source_mode({'synthetic': False}))"
        )
        env = {**os.environ, "MATRIXARK_CONTEXT_SOURCE_MODE": "remote_only"}
        repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        out = subprocess.run(
            [sys.executable, "-c", code], cwd=repo_root, env=env, capture_output=True, text=True
        )
        self.assertEqual(0, out.returncode, out.stderr)
        self.assertEqual("remote_only remote_only", out.stdout.strip())

    def test_synthetic_classifier_drives_population(self):
        # What the hook feeds into retrieve_args["synthetic"].
        self.assertTrue(is_synthetic_hook_text("probe smoke test"))
        self.assertTrue(is_synthetic_hook_text("matrixark synthetic hook capture"))
        self.assertFalse(is_synthetic_hook_text("What did the user decide about Rust hooks?"))


class RemoteOnlyLocalFallbackFloorTest(unittest.TestCase):
    def test_sparse_remote_pack_readmits_local(self):
        items = [{"ref_type": "local_context", "text": "user is on Windows", "token_estimate": 40}]
        lb = _remote_only_budget(items, observed_tokens=40)
        applied = apply_remote_only_local_fallback(lb, used_remote_tokens=12, floor_tokens=256)
        self.assertTrue(applied)
        self.assertEqual(items, lb["items"])                     # local re-admitted
        self.assertEqual(40, lb["token_estimate"])               # reservation restored
        self.assertEqual("remote_only_local_fallback", lb["context_source_mode"])

    def test_adequate_remote_pack_keeps_remote_only(self):
        lb = _remote_only_budget([{"text": "x", "token_estimate": 40}], observed_tokens=40)
        self.assertFalse(apply_remote_only_local_fallback(lb, used_remote_tokens=900, floor_tokens=256))
        self.assertEqual([], lb["items"])                        # stays remote-only
        self.assertEqual("remote_only", lb["context_source_mode"])

    def test_real_local_and_remote_request_is_never_touched(self):
        lb = {"context_source_mode": "local_and_remote", "items": [{"text": "x"}], "token_estimate": 40}
        self.assertFalse(apply_remote_only_local_fallback(lb, used_remote_tokens=0, floor_tokens=256))
        self.assertEqual("local_and_remote", lb["context_source_mode"])

    def test_floor_zero_disables_fallback(self):
        lb = _remote_only_budget([{"text": "x", "token_estimate": 40}], observed_tokens=40)
        self.assertFalse(apply_remote_only_local_fallback(lb, used_remote_tokens=0, floor_tokens=0))

    def test_no_local_to_fall_back_to_is_a_noop(self):
        lb = _remote_only_budget([], observed_tokens=0)
        self.assertFalse(apply_remote_only_local_fallback(lb, used_remote_tokens=0, floor_tokens=256))

    def test_default_floor_constant(self):
        self.assertEqual(256, cfg.DEFAULT_REMOTE_ONLY_LOCAL_FALLBACK_FLOOR_TOKENS)


import matrixark_mcp_budget_policies as bpol


class ModeDependentQuotaTest(unittest.TestCase):
    """Augment routes the memory budget to cross-session; remote-only keeps it a minority."""

    def _ratio(self, mode):
        pol = bpol.build_cross_session_policy(
            {"query": "what did we decide"}, {},
            question_type="fact", session_scope="prefer",
            remote_budget_tokens=10000, context_source_mode=mode,
        )
        return pol["budget_ratio"]

    def test_flag_off_is_legacy_regardless_of_mode(self):
        # Default OFF: mode does not change the ratio (preserves existing behavior + tests).
        orig = bpol.MODE_DEPENDENT_QUOTA_ENABLED
        bpol.MODE_DEPENDENT_QUOTA_ENABLED = False
        try:
            self.assertEqual(0.12, self._ratio("local_and_remote"))
            self.assertEqual(0.12, self._ratio("remote_only"))
        finally:
            bpol.MODE_DEPENDENT_QUOTA_ENABLED = orig

    def test_flag_on_augment_routes_to_cross_session(self):
        orig = bpol.MODE_DEPENDENT_QUOTA_ENABLED
        bpol.MODE_DEPENDENT_QUOTA_ENABLED = True
        try:
            self.assertEqual(0.60, self._ratio("local_and_remote"))  # cross-session gets the memory budget
        finally:
            bpol.MODE_DEPENDENT_QUOTA_ENABLED = orig

    def test_flag_on_remote_only_reserves_for_current_session(self):
        orig = bpol.MODE_DEPENDENT_QUOTA_ENABLED
        bpol.MODE_DEPENDENT_QUOTA_ENABLED = True
        try:
            self.assertEqual(0.30, self._ratio("remote_only"))       # current-session reconstruction gets the majority
        finally:
            bpol.MODE_DEPENDENT_QUOTA_ENABLED = orig

    def test_flag_on_no_mode_is_legacy(self):
        orig = bpol.MODE_DEPENDENT_QUOTA_ENABLED
        bpol.MODE_DEPENDENT_QUOTA_ENABLED = True
        try:
            self.assertEqual(0.12, self._ratio(""))                  # no mode → unchanged
        finally:
            bpol.MODE_DEPENDENT_QUOTA_ENABLED = orig

    def test_quota_constants(self):
        self.assertEqual(0.60, cfg.DEFAULT_AUGMENT_CROSS_SESSION_BUDGET_RATIO)
        self.assertEqual(0.30, cfg.DEFAULT_REMOTE_ONLY_CROSS_SESSION_BUDGET_RATIO)
        self.assertFalse(cfg.MODE_DEPENDENT_QUOTA_ENABLED)           # ships OFF


if __name__ == "__main__":
    unittest.main()
