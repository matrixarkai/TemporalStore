#!/usr/bin/env python3
from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from tools.matrixark_resource_parser import ResourceParserError, embedding_text_for_chunk, parse_resource, summarize_resource_chunks
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
        self.assertEqual(chunks[1].source_ref, "runbook.md#heading=rollback/checks")
        self.assertEqual(chunks[1].metadata["heading_level"], 2)
        self.assertEqual(chunks[1].metadata["heading_path"], ["Rollback", "Checks"])
        self.assertIn("content_hash", chunks[1].metadata)
        self.assertIn("keywords", chunks[1].metadata)
        embedding_text = embedding_text_for_chunk(chunks[1])
        self.assertIn("path: Rollback / Checks", embedding_text)
        self.assertIn("source: runbook.md#heading=rollback/checks", embedding_text)
        self.assertIn("content: ## Checks", embedding_text)
        l0_source = summarize_resource_chunks(chunks, raw_uri="runbook.md")
        self.assertIn("resource: runbook.md", l0_source)
        self.assertIn("section: Rollback / Checks", l0_source)

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

    def test_token_aware_splitter_versions_and_supersedes_chunks(self):
        text = " ".join(f"token{i}" for i in range(95))
        chunks = parse_resource(
            "long.txt",
            resource_type="txt",
            text=text,
            max_chunk_tokens=32,
            overlap_tokens=4,
            max_chunk_chars=4096,
            chunk_hash_base=3000,
            resource_version="v2",
            supersedes_chunk_hashes={"long.txt#paragraph=0": 2999},
        )
        self.assertGreater(len(chunks), 2)
        self.assertTrue(all(chunk.token_estimate <= 32 for chunk in chunks))
        self.assertEqual(chunks[0].metadata["resource_version"], "v2")
        self.assertEqual(chunks[0].metadata["supersedes_chunk_hash"], 2999)
        self.assertIn("resource_version", chunks[0].metadata["embedding_text"])

    def test_resource_version_defaults_to_content_hash(self):
        first = parse_resource("same.txt", resource_type="txt", text="same body", chunk_hash_base=3100)
        second = parse_resource("same.txt", resource_type="txt", text="same body", chunk_hash_base=3200)
        changed = parse_resource("same.txt", resource_type="txt", text="changed body", chunk_hash_base=3300)
        self.assertEqual(first[0].metadata["resource_version"], second[0].metadata["resource_version"])
        self.assertNotEqual(first[0].metadata["resource_version"], changed[0].metadata["resource_version"])

    def test_pdf_text_fixture_without_pdf_header_uses_text_fallback(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "sample.pdf"
            path.write_text("Page one text.\n\nPage two text.", encoding="utf-8")
            chunks = parse_resource(path, resource_type="pdf", chunk_hash_base=800)
        self.assertEqual(len(chunks), 2)
        self.assertEqual(chunks[0].chunk_hash, 800)
        self.assertEqual(chunks[0].metadata["resource_type"], "pdf")
        self.assertEqual(chunks[0].metadata["unit_kind"], "pdf_text_fallback")

    def test_real_pdf_file_is_parsed_when_pdf_dependencies_exist(self):
        try:
            from reportlab.pdfgen import canvas  # type: ignore
            import pypdf  # noqa: F401  # type: ignore
        except Exception as exc:
            raise unittest.SkipTest(f"PDF generation/parsing dependency unavailable: {exc}")

        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "real.pdf"
            pdf = canvas.Canvas(str(path))
            pdf.drawString(72, 720, "Finance approved the GPU budget.")
            pdf.showPage()
            pdf.drawString(72, 720, "Rollback requires health checks.")
            pdf.save()
            chunks = parse_resource(path, resource_type="pdf", chunk_hash_base=1300)

        self.assertEqual([chunk.source_ref for chunk in chunks], [f"{path}#page=1", f"{path}#page=2"])
        self.assertEqual([chunk.chunk_hash for chunk in chunks], [1300, 1301])
        self.assertIn("Finance approved", chunks[0].text)

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
        self.assertEqual(skill.metadata["skill_slug"], "context-debugger")
        self.assertIn("skill: context-debugger", skill.metadata["embedding_text"])
        self.assertIn("allowed_tools: matrixark_replay, matrixark_audit", skill.metadata["embedding_text"])
        self.assertEqual(skill.metadata["section_count"], len(skill.chunks))
        self.assertEqual(skill.chunks[0].chunk_hash, 1200)
        self.assertEqual(skill.chunks[0].metadata["resource_type"], "skill")

    def test_html_csv_jsonl_resources_are_parsed_with_metadata(self):
        html_chunks = parse_resource(
            "page.html",
            resource_type="html",
            text="<html><title>Ops</title><body><h1>GPU</h1><p>Finance approval required.</p></body></html>",
            chunk_hash_base=1500,
        )
        self.assertEqual(html_chunks[0].metadata["title"], "Ops")
        self.assertEqual(html_chunks[0].metadata["unit_kind"], "html_text")
        self.assertTrue(any("Finance approval" in chunk.text for chunk in html_chunks))

        csv_chunks = parse_resource(
            "budget.csv",
            resource_type="csv",
            text="item,amount,approver\ngpu,42000,Alice\n",
            chunk_hash_base=1600,
        )
        self.assertEqual(csv_chunks[0].source_ref, "budget.csv#row=0")
        self.assertEqual(csv_chunks[0].metadata["columns"], ["item", "amount", "approver"])

        jsonl_chunks = parse_resource(
            "events.jsonl",
            resource_type="jsonl",
            text='{"event":"approval","actor":"Alice"}\n',
            chunk_hash_base=1700,
        )
        self.assertEqual(jsonl_chunks[0].source_ref, "events.jsonl#record=0")
        self.assertEqual(jsonl_chunks[0].metadata["unit_kind"], "jsonl_record")

    def test_skill_section_lists_and_permissions_are_extracted(self):
        skill = parse_skill(
            "skills/ops/SKILL.md",
            text=(
                "# Ops Helper\n\n"
                "Helps with production operations.\n\n"
                "## Triggers\n\n"
                "- production incident\n\n"
                "## Tools\n\n"
                "- matrixark_retrieve\n\n"
                "## Permissions\n\n"
                "- context:retrieve\n\n"
                "## Inputs\n\n"
                "- incident summary\n"
            ),
            chunk_hash_base=1800,
        )
        self.assertEqual(skill.name, "Ops Helper")
        self.assertEqual(skill.metadata["triggers"], ["production incident"])
        self.assertEqual(skill.metadata["allowed_tools"], ["matrixark_retrieve"])
        self.assertEqual(skill.metadata["permissions"], ["context:retrieve"])
        self.assertEqual(skill.metadata["inputs"], ["incident summary"])


    def test_directory_resource_parses_supported_child_files(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "resources"
            root.mkdir()
            (root / "runbook.md").write_text("# GPU\n\nFinance approval required.", encoding="utf-8")
            (root / "facts.jsonl").write_text('{"fact":"CFO approval over 40000"}\n', encoding="utf-8")
            (root / "ignore.bin").write_bytes(b"not parsed")
            chunks = parse_resource(root, resource_type="directory", chunk_hash_base=1900)
        self.assertEqual([chunk.chunk_hash for chunk in chunks], [1900, 1901])
        self.assertEqual(chunks[0].metadata["relative_path"], "facts.jsonl")
        self.assertEqual(chunks[1].metadata["relative_path"], "runbook.md")
        self.assertEqual(chunks[1].metadata["child_resource_type"], "md")
        self.assertIn("runbook.md#heading=gpu", chunks[1].source_ref)

    def test_skill_bundle_reads_manifest_and_readme_fallback(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "skill_bundle"
            root.mkdir()
            (root / "manifest.json").write_text(
                '{"name":"bundle-debugger","description":"Bundle manifest description.","triggers":["bundle replay"],"tools":["matrixark_replay"],"version":"3"}',
                encoding="utf-8",
            )
            (root / "README.md").write_text(
                "# Bundle Debugger\n\nREADME body.\n\n## Permissions\n\n- context:retrieve\n",
                encoding="utf-8",
            )
            skill = parse_skill(root, chunk_hash_base=2000)
        self.assertEqual(skill.name, "bundle-debugger")
        self.assertEqual(skill.description, "Bundle manifest description.")
        self.assertEqual(skill.metadata["triggers"], ["bundle replay"])
        self.assertEqual(skill.metadata["allowed_tools"], ["matrixark_replay"])
        self.assertEqual(skill.metadata["permissions"], ["context:retrieve"])
        self.assertEqual(skill.metadata["version"], "3")
        self.assertIn("README.md", skill.metadata["bundle_files"])
        self.assertTrue(skill.metadata["bundle_manifest"].get("manifest_uri"))


    def test_production_limits_reject_large_file_and_too_many_directory_files(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            large = root / "large.txt"
            large.write_text("x" * 64, encoding="utf-8")
            with self.assertRaises(ResourceParserError):
                parse_resource(large, resource_type="txt", max_file_bytes=16)

            directory = root / "many"
            directory.mkdir()
            (directory / "a.txt").write_text("one", encoding="utf-8")
            (directory / "b.txt").write_text("two", encoding="utf-8")
            with self.assertRaises(ResourceParserError):
                parse_resource(directory, resource_type="directory", max_directory_files=1)

    def test_directory_resource_skips_vendor_and_hidden_dirs(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "resources"
            root.mkdir()
            (root / "good.txt").write_text("good context", encoding="utf-8")
            hidden = root / ".git"
            hidden.mkdir()
            (hidden / "ignored.txt").write_text("ignored", encoding="utf-8")
            vendor = root / "node_modules"
            vendor.mkdir()
            (vendor / "ignored.txt").write_text("ignored", encoding="utf-8")
            chunks = parse_resource(root, resource_type="directory", chunk_hash_base=2100)
        self.assertEqual(len(chunks), 1)
        self.assertEqual(chunks[0].metadata["relative_path"], "good.txt")
        self.assertGreaterEqual(chunks[0].metadata["directory_skipped_files"], 2)


    def test_production_limits_reject_inline_text_unsupported_files_and_hidden_leafs(self):
        with self.assertRaises(ResourceParserError):
            parse_resource("inline.txt", resource_type="txt", text="x" * 32, max_inline_text_chars=16)

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "resources"
            root.mkdir()
            (root / "good.txt").write_text("good context", encoding="utf-8")
            (root / ".env").write_text("secret", encoding="utf-8")
            (root / "binary.exe").write_bytes(b"MZ")
            with self.assertRaises(ResourceParserError):
                parse_resource(root / "binary.exe")
            chunks = parse_resource(root, resource_type="directory", chunk_hash_base=2300)
        self.assertEqual(len(chunks), 1)
        self.assertEqual(chunks[0].metadata["relative_path"], "good.txt")
        self.assertGreaterEqual(chunks[0].metadata["directory_skipped_files"], 2)

    def test_skill_bundle_skips_hidden_files_and_enforces_text_limit(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "skill_bundle"
            root.mkdir()
            (root / "SKILL.md").write_text("# Good Skill\n\nBody.", encoding="utf-8")
            (root / ".secret").write_text("hidden", encoding="utf-8")
            vendor = root / "node_modules"
            vendor.mkdir()
            (vendor / "ignored.md").write_text("ignored", encoding="utf-8")
            skill = parse_skill(root, chunk_hash_base=2400)
        self.assertEqual(skill.metadata["bundle_files"], ["SKILL.md"])

        with self.assertRaises(ValueError):
            parse_skill("skills/too-large/SKILL.md", text="# Big\n" + ("x" * 32), max_text_chars=16)
        with self.assertRaises(ValueError):
            parse_skill("skills/bad/SKILL.md", text="# Bad", max_text_chars=0)

    def test_skill_invalid_status_scope_precedence_default_safely(self):
        skill = parse_skill(
            "skills/bad/SKILL.md",
            text=(
                "---\n"
                "name: bad-skill\n"
                "status: unknown\n"
                "precedence: urgent\n"
                "owner_scope: private\n"
                "---\n"
                "# Bad Skill\n\nBody."
            ),
            chunk_hash_base=2200,
        )
        self.assertEqual(skill.metadata["status"], "active")
        self.assertEqual(skill.metadata["precedence"], "normal")
        self.assertEqual(skill.metadata["owner_scope"], "user")

    def test_skill_codex_nested_metadata_short_description(self):
        skill = parse_skill(
            "skills/codex/SKILL.md",
            text=(
                "---\n"
                "name: codex-context\n"
                "metadata:\n"
                "  short-description: Use Codex context safely.\n"
                "---\n"
                "# Codex Context\n\n"
                "Longer body for the skill.\n"
            ),
            chunk_hash_base=2500,
        )
        self.assertEqual(skill.description, "Use Codex context safely.")
        self.assertEqual(skill.metadata["front_matter"]["metadata"]["short-description"], "Use Codex context safely.")
        self.assertIn("description: Use Codex context safely.", skill.metadata["embedding_text"])

if __name__ == "__main__":
    unittest.main()
