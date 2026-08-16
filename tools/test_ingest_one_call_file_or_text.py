#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Tests for single-call resource/skill ingest accepting a local file OR inline
text without a dummy ``messages`` list (front-door envelope contract only)."""
import os
import tempfile
import unittest

from matrixark_mcp_core import (
    MatrixArkError,
    normalize_envelope,
    resolve_ingest_messages,
)
from matrixark_mcp_core_resource_io import resolve_raw_resource_for_ingest

SCOPE = {"tenant_id": "t1", "user_id": "u1"}


class FileOnlyCallTest(unittest.TestCase):
    def test_file_only_synthesizes_messages_and_preserves_raw_uri(self):
        env = normalize_envelope(
            {"kind": "resource", "raw_uri": "/tmp/some.md", "resource_type": "md", "scope": SCOPE},
            default_kind="resource",
        )
        self.assertEqual(env["kind"], "resource")
        self.assertIsInstance(env["messages"], list)
        self.assertTrue(env["messages"])  # non-empty synthesized list
        self.assertEqual(env["messages"], [{"role": "user", "content": "resource:/tmp/some.md"}])
        self.assertEqual(env["raw_uri"], "/tmp/some.md")


class TextOnlyCallTest(unittest.TestCase):
    def test_text_only(self):
        env = normalize_envelope(
            {"kind": "resource", "text": "hello world", "resource_type": "md", "scope": SCOPE},
            default_kind="resource",
        )
        self.assertEqual(env["messages"], [{"role": "user", "content": "hello world"}])

    def test_resource_text_synonym(self):
        env = normalize_envelope(
            {"kind": "resource", "resource_text": "synonym body", "resource_type": "md", "scope": SCOPE},
            default_kind="resource",
        )
        self.assertEqual(env["messages"], [{"role": "user", "content": "synonym body"}])

    def test_text_wins_over_resource_text(self):
        env = normalize_envelope(
            {"kind": "resource", "text": "primary", "resource_text": "secondary", "scope": SCOPE},
            default_kind="resource",
        )
        self.assertEqual(env["messages"], [{"role": "user", "content": "primary"}])


class SkillKindTest(unittest.TestCase):
    def test_skill_file_only(self):
        env = normalize_envelope(
            {"kind": "skill", "raw_uri": "/tmp/skill.md", "scope": SCOPE},
            default_kind="skill",
        )
        self.assertEqual(env["messages"], [{"role": "user", "content": "resource:/tmp/skill.md"}])

    def test_skill_text_only(self):
        env = normalize_envelope(
            {"kind": "skill", "text": "do the thing", "scope": SCOPE},
            default_kind="skill",
        )
        self.assertEqual(env["messages"], [{"role": "user", "content": "do the thing"}])


class BackwardCompatTest(unittest.TestCase):
    def test_resource_with_messages_unchanged(self):
        env = normalize_envelope(
            {
                "kind": "resource",
                "messages": [{"role": "user", "content": "x"}],
                "raw_uri": "/tmp/some.md",
                "scope": SCOPE,
            },
            default_kind="resource",
        )
        self.assertEqual(env["messages"], [{"role": "user", "content": "x"}])
        self.assertEqual(env["raw_uri"], "/tmp/some.md")

    def test_normal_message_ingest_unchanged(self):
        env = normalize_envelope(
            {"kind": "message", "messages": [{"role": "user", "content": "hi"}], "scope": SCOPE},
            default_kind="message",
        )
        self.assertEqual(env["kind"], "message")
        self.assertEqual(env["messages"], [{"role": "user", "content": "hi"}])

    def test_messages_take_precedence_over_text(self):
        env = normalize_envelope(
            {
                "kind": "resource",
                "messages": [{"role": "user", "content": "from-messages"}],
                "text": "from-text",
                "scope": SCOPE,
            },
            default_kind="resource",
        )
        self.assertEqual(env["messages"], [{"role": "user", "content": "from-messages"}])


class ErrorTest(unittest.TestCase):
    def test_resource_no_source_raises(self):
        with self.assertRaises(MatrixArkError) as ctx:
            normalize_envelope({"kind": "resource", "scope": SCOPE}, default_kind="resource")
        self.assertIn("resource/skill ingest needs one of", str(ctx.exception))

    def test_resource_inline_placeholder_rawuri_raises(self):
        # raw_uri == "inline-resource" is not a real source -> must not synthesize a placeholder
        with self.assertRaises(MatrixArkError):
            normalize_envelope(
                {"kind": "resource", "raw_uri": "inline-resource", "scope": SCOPE},
                default_kind="resource",
            )

    def test_message_no_messages_still_strict(self):
        with self.assertRaises(MatrixArkError) as ctx:
            normalize_envelope({"kind": "message", "scope": SCOPE}, default_kind="message")
        self.assertIn("messages must be a non-empty list", str(ctx.exception))

    def test_business_data_no_messages_still_strict(self):
        with self.assertRaises(MatrixArkError):
            normalize_envelope({"kind": "business_data", "scope": SCOPE}, default_kind="message")


class EndToEndResolverTest(unittest.TestCase):
    """Prove a one-call synthesized envelope resolves through the (unchanged)
    resolver without any messages supplied by the caller."""

    def test_file_only_resolves_parse_uri_to_file(self):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "doc.md")
            with open(path, "w", encoding="utf-8") as f:
                f.write("# real file body\n")
            env = normalize_envelope(
                {
                    "kind": "resource",
                    "raw_uri": path,
                    "resource_type": "md",
                    "raw_storage_mode": "local",
                    "scope": SCOPE,
                },
                default_kind="resource",
            )
            # runtime derives resource_text from messages (the harmless placeholder)
            resource_text = "\n\n".join(str(m["content"]) for m in env["messages"])
            res = resolve_raw_resource_for_ingest(
                {"raw_storage_mode": "local"}, env, path, "md", "local", resource_text
            )
            self.assertEqual(res["parse_uri"], path)
            self.assertIsNone(res["parse_text"])  # file is parsed, placeholder ignored

    def test_text_only_resource_text_carries_through(self):
        env = normalize_envelope(
            {
                "kind": "resource",
                "text": "inline knowledge",
                "resource_type": "md",
                "raw_storage_mode": "local",
                "scope": SCOPE,
            },
            default_kind="resource",
        )
        resource_text = "\n\n".join(str(m["content"]) for m in env["messages"])
        self.assertEqual(resource_text, "inline knowledge")
        res = resolve_raw_resource_for_ingest(
            {"raw_storage_mode": "local"}, env, "inline-resource", "md", "local", resource_text
        )
        self.assertEqual(res["parse_text"], "inline knowledge")


class HelperUnitTest(unittest.TestCase):
    def test_resolve_ingest_messages_direct(self):
        self.assertEqual(
            resolve_ingest_messages({"text": "abc"}, "resource"),
            [{"role": "user", "content": "abc"}],
        )
        self.assertEqual(
            resolve_ingest_messages({"messages": [{"role": "user", "content": "z"}]}, "message"),
            [{"role": "user", "content": "z"}],
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
