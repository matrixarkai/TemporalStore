#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Backfill ingests tool events, but leaned (small text / pre-analyzed)."""
import unittest
import json
import os
import sys
import tempfile
from pathlib import Path
from unittest.mock import patch

import matrixark_local_backfill_ingester as B


class ToolLeaningTest(unittest.TestCase):
    def test_tool_use_becomes_name_plus_key_arg(self):
        self.assertEqual("[tool:Bash] ls -la /tmp",
                         B._lean_tool_block({"type": "tool_use", "name": "Bash", "input": {"command": "ls -la /tmp"}}))
        self.assertEqual("[tool:Read] /a/b.py",
                         B._lean_tool_block({"type": "tool_use", "name": "Read", "input": {"file_path": "/a/b.py", "limit": 9}}))

    def test_tool_result_is_head_truncated(self):
        big = {"type": "tool_result", "content": [{"type": "text", "text": "x " * 1000}]}
        out = B._lean_tool_block(big)
        self.assertTrue(out.startswith("[tool_result] "))
        self.assertLessEqual(len(out), B._TOOL_LEAN_CHARS + 40)   # leaned, not the full dump
        self.assertIn("chars]", out)                              # elision marker

    def test_short_tool_result_kept_whole(self):
        self.assertEqual("[tool_result] 42 passed, 1 failed",
                         B._lean_tool_block({"type": "tool_result", "content": "42 passed, 1 failed"}))

    def test_content_to_text_includes_tool_blocks_leaned(self):
        mix = B._content_to_text([
            {"type": "text", "text": "hello"},
            {"type": "tool_use", "name": "Grep", "input": {"pattern": "foo"}},
            {"type": "tool_result", "content": "done"},
        ])
        self.assertEqual("hello\n[tool:Grep] foo\n[tool_result] done", mix)

    def test_non_tool_still_plain(self):
        self.assertEqual("just text", B._content_to_text([{"type": "text", "text": "just text"}]))

    def test_agent_roots_default_to_durable_hook_stores(self):
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual("/root/.matrixark/temporalstore-hooks/claude/rust", B.agent_root("claude"))
            self.assertEqual("/root/.matrixark/temporalstore-hooks/codex/rust", B.agent_root("codex"))

    def test_codex_agent_root_honors_live_hook_root_override(self):
        with patch.dict(os.environ, {"TEMPORALSTORE_RUST_CODEX_HOOK_ROOT": "/tmp/live-codex-root"}, clear=True):
            self.assertEqual("/tmp/live-codex-root", B.agent_root("codex"))

    def test_dedicated_codex_agent_root_wins_over_shared_override(self):
        with patch.dict(
            os.environ,
            {
                "MATRIXARK_CODEX_RUST_HOOK_ROOT": "/tmp/dedicated-codex-root",
                "TEMPORALSTORE_RUST_CODEX_HOOK_ROOT": "/tmp/live-codex-root",
            },
            clear=True,
        ):
            self.assertEqual("/tmp/dedicated-codex-root", B.agent_root("codex"))

    def test_emit_jsonl_streams_local_codex_and_claude_context(self):
        with tempfile.TemporaryDirectory() as td:
            home = Path(td) / "home"
            out = Path(td) / "emit"
            claude_project = home / ".claude" / "projects" / "proj"
            codex_session = home / ".codex" / "sessions" / "2026" / "08" / "13"
            claude_project.mkdir(parents=True)
            codex_session.mkdir(parents=True)
            (claude_project / "claude-session.jsonl").write_text(
                json.dumps(
                    {
                        "timestamp": "2026-08-13T12:00:00Z",
                        "message": {"role": "user", "content": [{"type": "text", "text": "remember claude fact"}]},
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            (codex_session / "rollout-codex-session.jsonl").write_text(
                json.dumps({"type": "session_meta", "payload": {"id": "codex-session"}})
                + "\n"
                + json.dumps(
                    {
                        "type": "response_item",
                        "timestamp": "2026-08-13T12:01:00Z",
                        "payload": {
                            "type": "message",
                            "role": "user",
                            "content": [{"type": "input_text", "text": "remember codex fact"}],
                        },
                    }
                )
                + "\n",
                encoding="utf-8",
            )

            argv = [
                "matrixark_local_backfill_ingester.py",
                "--home",
                str(home),
                "--agents",
                "claude,codex",
                "--sources",
                "transcripts,rollouts",
                "--emit-jsonl",
                str(out),
            ]
            with patch.object(sys, "argv", argv):
                self.assertEqual(0, B.main())

            claude_rows = (out / "backfill.claude.jsonl").read_text(encoding="utf-8").splitlines()
            codex_rows = (out / "backfill.codex.jsonl").read_text(encoding="utf-8").splitlines()
            self.assertEqual(1, len(claude_rows))
            self.assertEqual(1, len(codex_rows))
            self.assertEqual("remember claude fact", json.loads(claude_rows[0])["text"])
            self.assertEqual("remember codex fact", json.loads(codex_rows[0])["text"])


if __name__ == "__main__":
    unittest.main()
