#!/usr/bin/env python3
from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from tools.matrixark_resource_parser import parse_resource
from tools.matrixark_skill_parser import parse_skill


class MatrixArkResourceParserTest(unittest.TestCase):
    def test_markdown_headings_become_stable_source_refs(self):
        chunks = parse_resource(
            "runbook.md",
            resource_type="md",
            text="# Rollback\n\nUse canary rollback.\n\n## Checks\n\nConfirm p95 latency.",
            chunk_hash_base=900,
        )
        self.assertEqual([chunk.chunk_hash for chunk in chunks], [900, 901])
        self.assertEqual(chunks[0].source_ref, "runbook.md#heading=rollback")
        self.assertEqual(chunks[1].source_ref, "runbook.md#heading=checks")
        self.assertEqual(chunks[1].metadata["heading_level"], 2)

    def test_text_paragraphs_are_chunked_with_refs(self):
        chunks = parse_resource(
            "notes.txt",
            resource_type="txt",
            text="Alice approved the request.\n\nFinance reviewed the budget.",
            chunk_hash_base=700,
        )
        self.assertEqual(len(chunks), 2)
        self.assertEqual(chunks[0].source_ref, "notes.txt#paragraph=0")
        self.assertEqual(chunks[1].source_ref, "notes.txt#paragraph=1")
        self.assertGreaterEqual(chunks[0].token_estimate, 1)

    def test_pdf_text_fixture_without_pdf_header_uses_text_fallback(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "sample.pdf"
            path.write_text("Page one text.\n\nPage two text.", encoding="utf-8")
            chunks = parse_resource(path, resource_type="pdf", chunk_hash_base=800)
        self.assertEqual(len(chunks), 2)
        self.assertEqual(chunks[0].chunk_hash, 800)
        self.assertEqual(chunks[0].metadata["resource_type"], "pdf")

    def test_skill_front_matter_and_chunks(self):
        skill = parse_skill(
            "skills/context/SKILL.md",
            text=(
                "---\n"
                "name: context-debugger\n"
                "description: Debug MatrixArk context packs.\n"
                "triggers:\n"
                "  - context pack replay\n"
                "allowed_tools: [matrixark_replay, matrixark_audit]\n"
                "version: 2\n"
                "---\n"
                "# Context Debugger\n\n"
                "Use this skill to inspect selected refs.\n\n"
                "## Steps\n\n"
                "Open replay and verify evidence."
            ),
            chunk_hash_base=1200,
        )
        self.assertEqual(skill.name, "context-debugger")
        self.assertEqual(skill.description, "Debug MatrixArk context packs.")
        self.assertEqual(skill.metadata["triggers"], ["context pack replay"])
        self.assertEqual(skill.metadata["allowed_tools"], ["matrixark_replay", "matrixark_audit"])
        self.assertEqual(skill.metadata["version"], "2")
        self.assertEqual(skill.chunks[0].chunk_hash, 1200)
        self.assertEqual(skill.chunks[0].metadata["resource_type"], "skill")


if __name__ == "__main__":
    unittest.main()
