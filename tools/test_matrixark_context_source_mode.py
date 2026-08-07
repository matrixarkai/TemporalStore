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


if __name__ == "__main__":
    unittest.main()
