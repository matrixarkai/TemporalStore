#!/usr/bin/env python3
"""Backfill ingests tool events, but leaned (small text / pre-analyzed)."""
import unittest

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


if __name__ == "__main__":
    unittest.main()
