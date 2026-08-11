#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Guard: a message-ingest scope without tenant_id/user_id must not silently disable profile."""
import tempfile
import unittest
import warnings
from pathlib import Path

import matrixark_mcp_local_adapter as mcp
import matrixark_local_adapter_ingest as ing


class ProfileScopeWarningTest(unittest.TestCase):
    def _adapter(self):
        return mcp.MatrixArkLocalAdapter(Path(tempfile.mkdtemp()) / "events.jsonl")

    def test_helper_flags_missing_and_passes_present(self):
        self.assertTrue(ing.warn_if_profile_scope_missing({"user_id": "u"}))          # no tenant_id
        self.assertTrue(ing.warn_if_profile_scope_missing({"tenant_id": "t"}))        # no user_id
        self.assertTrue(ing.warn_if_profile_scope_missing({"tenant_id": "t", "user_id": ""}))  # empty
        self.assertEqual("", ing.warn_if_profile_scope_missing({"tenant_id": "t", "user_id": "u"}))
        self.assertEqual("", ing.warn_if_profile_scope_missing("not a dict"))

    def test_ingest_missing_scope_warns_and_surfaces(self):
        ing._PROFILE_SCOPE_WARNED.clear()
        a = self._adapter()
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            r = a.ingest({"kind": "message", "scope": {"user": "alice", "session_id": "s1"},
                          "messages": [{"role": "user", "content": "I am Alice, ML lead."}]})
        self.assertEqual(1, len(w))
        self.assertIn("profile_scope_missing", str(w[0].message))
        self.assertIn("profile_scope_warning", r)

    def test_ingest_full_scope_is_silent(self):
        ing._PROFILE_SCOPE_WARNED.clear()
        a = self._adapter()
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            r = a.ingest({"kind": "message", "scope": {"tenant_id": "acme", "user_id": "alice", "session_id": "s2"},
                          "messages": [{"role": "user", "content": "hi"}]})
        self.assertEqual(0, len(w))
        self.assertNotIn("profile_scope_warning", r)

    def test_warning_deduped_per_identity(self):
        ing._PROFILE_SCOPE_WARNED.clear()
        a = self._adapter()
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            for _ in range(3):
                a.ingest({"kind": "message", "scope": {"session_id": "s"},
                          "messages": [{"role": "user", "content": "x"}]})
        self.assertEqual(1, len(w))  # warned once, not per-call


class BatchedIngestWarningTest(unittest.TestCase):
    def _adapter(self):
        return mcp.MatrixArkLocalAdapter(Path(tempfile.mkdtemp()) / "events.jsonl")

    def test_helper_flags_large_batch_only(self):
        ing._BATCH_MESSAGES_WARNED.clear()
        self.assertEqual("", ing.warn_if_batched_messages([{"role": "user", "content": "x"}] * 2))
        self.assertTrue(ing.warn_if_batched_messages([{"role": "user", "content": "x"}] * 12))

    def test_ingest_large_batch_warns_and_surfaces(self):
        ing._BATCH_MESSAGES_WARNED.clear()
        a = self._adapter()
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            r = a.ingest({"kind": "message", "scope": {"tenant_id": "t", "user_id": "u", "session_id": "s"},
                          "messages": [{"role": "user", "content": f"m{i}"} for i in range(12)]})
        self.assertTrue(any("batched_ingest" in str(x.message) for x in w))
        self.assertIn("batched_ingest_warning", r)

    def test_per_turn_ingest_is_silent(self):
        ing._BATCH_MESSAGES_WARNED.clear()
        a = self._adapter()
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            r = a.ingest({"kind": "message", "scope": {"tenant_id": "t", "user_id": "u", "session_id": "s"},
                          "messages": [{"role": "user", "content": "hi"}, {"role": "assistant", "content": "ok"}]})
        self.assertFalse(any("batched_ingest" in str(x.message) for x in w))
        self.assertNotIn("batched_ingest_warning", r)


if __name__ == "__main__":
    unittest.main()
