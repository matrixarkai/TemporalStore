#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Regression tests for the Codex plugin session resolver."""

from __future__ import annotations

import json
import subprocess
import textwrap
import unittest
from pathlib import Path


class MatrixArkCodexPluginSessionResolverTest(unittest.TestCase):
    def run_resolver(self, *, payload: dict, env: dict) -> dict:
        repo = Path(__file__).resolve().parents[1]
        script = textwrap.dedent(
            f"""
            import {{ resolveSessionId }} from "./integrations/agent-hooks/codex/plugin/scripts/session_resolver.mjs";
            const payload = {json.dumps(payload, sort_keys=True)};
            const env = {json.dumps(env, sort_keys=True)};
            console.log(JSON.stringify(resolveSessionId(payload, env)));
            """
        )
        proc = subprocess.run(
            ["node", "--input-type=module", "-e", script],
            cwd=repo,
            text=True,
            capture_output=True,
            timeout=10,
        )
        if proc.returncode != 0:
            raise AssertionError(f"resolver failed\nstdout={proc.stdout}\nstderr={proc.stderr}")
        return json.loads(proc.stdout)

    def run_normalizer(self, *, event: str, payload: dict, env: dict | None = None) -> dict:
        repo = Path(__file__).resolve().parents[1]
        script = textwrap.dedent(
            f"""
            import {{ normalizePayload }} from "./integrations/agent-hooks/codex/plugin/scripts/payload_normalizer.mjs";
            const payload = {json.dumps(payload, sort_keys=True)};
            const env = {json.dumps(env or {"TEMPORALSTORE_AGENT_NAME": "codex"}, sort_keys=True)};
            console.log(JSON.stringify(normalizePayload({{event: {json.dumps(event)}, payload, env}})));
            """
        )
        proc = subprocess.run(
            ["node", "--input-type=module", "-e", script],
            cwd=repo,
            text=True,
            capture_output=True,
            timeout=10,
        )
        if proc.returncode != 0:
            raise AssertionError(f"normalizer failed\nstdout={proc.stdout}\nstderr={proc.stderr}")
        return json.loads(proc.stdout)

    def test_payload_session_id_wins_over_environment_and_workspace(self) -> None:
        resolved = self.run_resolver(
            payload={"conversation_id": "payload-thread-7", "workspace_root": "/repo/shared"},
            env={"TEMPORALSTORE_AGENT_NAME": "codex", "CODEX_THREAD_ID": "env-thread-9"},
        )

        self.assertEqual("codex:payload-thread-7", resolved["sessionId"])
        self.assertEqual("payload-thread-7", resolved["conversationId"])
        self.assertEqual("payload.conversation_id", resolved["source"])

    def test_environment_thread_id_prevents_workspace_session_collapse(self) -> None:
        first = self.run_resolver(
            payload={"workspace_root": "/repo/shared"},
            env={"TEMPORALSTORE_AGENT_NAME": "codex", "CODEX_THREAD_ID": "thread-alpha"},
        )
        second = self.run_resolver(
            payload={"workspace_root": "/repo/shared"},
            env={"TEMPORALSTORE_AGENT_NAME": "codex", "CODEX_THREAD_ID": "thread-beta"},
        )

        self.assertEqual("codex:thread-alpha", first["sessionId"])
        self.assertEqual("thread-alpha", first["conversationId"])
        self.assertEqual("env.CODEX_THREAD_ID", first["source"])
        self.assertEqual("codex:thread-beta", second["sessionId"])
        self.assertEqual("env.CODEX_THREAD_ID", second["source"])
        self.assertNotEqual(first["sessionId"], second["sessionId"])

    def test_workspace_hash_is_last_resort_only(self) -> None:
        resolved = self.run_resolver(
            payload={"workspace_root": "/repo/shared"},
            env={"TEMPORALSTORE_AGENT_NAME": "codex", "MATRIXARK_USER_ID": "user-a"},
        )

        self.assertTrue(resolved["sessionId"].startswith("codex:local:"))
        self.assertTrue(resolved["conversationId"].startswith("local:"))
        self.assertEqual("workspace_hash", resolved["source"])

    def test_normalizer_preserves_assistant_response_text_and_role(self) -> None:
        normalized = self.run_normalizer(
            event="AssistantResponse",
            payload={
                "conversation_id": "codex-thread-llm",
                "assistant_message": "Decision: keep profile entities cross-session.",
                "workspace_root": "/repo/shared",
            },
        )

        self.assertEqual("assistant", normalized["role"])
        self.assertEqual("Decision: keep profile entities cross-session.", normalized["text"])
        self.assertEqual("codex:codex-thread-llm", normalized["session_id"])
        self.assertEqual("after_llm", normalized["hook_type"])
        self.assertEqual("after_llm_ingest", normalized["lifecycle_stage"])
        self.assertFalse(normalized["should_retrieve"])
        self.assertFalse(normalized["should_commit"])
        self.assertEqual("provisional", normalized["extraction_phase"])
        self.assertFalse(normalized["final_session_boundary"])

    def test_normalizer_preserves_tool_output_text_and_role(self) -> None:
        normalized = self.run_normalizer(
            event="PostToolUse",
            payload={
                "conversation_id": "codex-thread-tool",
                "tool": {"name": "cargo", "output": "Exit code: 0\nRan 3 tests in 0.30s\nOK"},
                "workspace_root": "/repo/shared",
            },
        )

        self.assertEqual("tool", normalized["role"])
        self.assertIn("Ran 3 tests", normalized["text"])
        self.assertEqual("codex:codex-thread-tool", normalized["session_id"])
        self.assertEqual("tool_result", normalized["hook_type"])
        self.assertEqual("tool_evidence_ingest", normalized["lifecycle_stage"])
        self.assertFalse(normalized["should_retrieve"])
        self.assertFalse(normalized["should_commit"])
        self.assertEqual("provisional", normalized["extraction_phase"])
        self.assertFalse(normalized["final_session_boundary"])

    def test_normalizer_marks_prompt_retrieval_and_stop_final_boundary(self) -> None:
        prompt = self.run_normalizer(
            event="UserPromptSubmit",
            payload={"conversation_id": "codex-thread-lifecycle", "prompt": "What should I do next?"},
        )
        stop = self.run_normalizer(
            event="Stop",
            payload={
                "conversation_id": "codex-thread-lifecycle",
                "last_agent_message": "Summary: commit final session memory.",
            },
        )

        self.assertEqual("before_llm", prompt["hook_type"])
        self.assertEqual("before_llm_retrieve", prompt["lifecycle_stage"])
        self.assertTrue(prompt["should_retrieve"])
        self.assertFalse(prompt["should_commit"])
        self.assertEqual("provisional", prompt["extraction_phase"])
        self.assertFalse(prompt["final_session_boundary"])
        self.assertEqual("session_commit", stop["hook_type"])
        self.assertEqual("session_boundary_commit", stop["lifecycle_stage"])
        self.assertFalse(stop["should_retrieve"])
        self.assertTrue(stop["should_commit"])
        self.assertEqual("final", stop["extraction_phase"])
        self.assertTrue(stop["final_session_boundary"])


if __name__ == "__main__":
    unittest.main()
