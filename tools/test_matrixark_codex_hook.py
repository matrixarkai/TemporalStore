import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
HOOK = ROOT / "tools" / "matrixark_codex_hook.py"


class MatrixArkCodexHookTest(unittest.TestCase):
    def run_hook(self, payload, *, event="UserPromptSubmit", event_log=None, query=""):
        event_log = event_log or Path(self.tmpdir.name) / "codex-hook.jsonl"
        proc = subprocess.run(
            [
                sys.executable,
                str(HOOK),
                "--event",
                event,
                "--event-log",
                str(event_log),
                "--account-id",
                "acct_test",
                "--tenant-id",
                "tenant_test",
                "--user-id",
                "codex-user",
                "--session-id",
                "codex-session",
                "--query",
                query,
            ],
            input=json.dumps(payload),
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            cwd=str(ROOT),
            check=True,
        )
        return json.loads(proc.stdout), event_log

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_user_prompt_ingests_and_retrieves_context(self):
        event_log = Path(self.tmpdir.name) / "codex-hook.jsonl"
        first, _ = self.run_hook(
            {"prompt": "Remember that Alice approved the GPU purchase for Project Orion."},
            event_log=event_log,
        )
        self.assertEqual(first["status"], "ok")
        self.assertEqual(first["event"], "UserPromptSubmit")
        self.assertGreaterEqual(first["retrieve"]["selected_ref_count"], 1)

        second, _ = self.run_hook(
            {"prompt": "What was approved for Project Orion?"},
            event_log=event_log,
        )
        self.assertEqual(second["status"], "ok")
        self.assertGreaterEqual(second["retrieve"]["selected_ref_count"], 1)

        records = [json.loads(line) for line in event_log.read_text().splitlines() if line.strip()]
        record_types = {record.get("record_type") for record in records}
        self.assertIn("context_event", record_types)
        self.assertIn("context_pack_audit", record_types)
        self.assertTrue(any(record.get("agent_hook", {}).get("source") == "codex" for record in records))

    def test_codex_session_stop_commits_multi_segment_memory(self):
        event_log = Path(self.tmpdir.name) / "codex-hook.jsonl"
        prompts = [
            "I moved to Seattle today, please remember this location.",
            "Actually I moved to Austin now for the new infra project.",
            "I prefer Rust for low latency storage engines.",
            "Alice approved the GPU purchase after finance reviewed the budget.",
        ]
        for prompt in prompts:
            result, _ = self.run_hook({"prompt": prompt}, event="UserPromptSubmit", event_log=event_log)
            self.assertEqual(result["status"], "ok")
            self.assertEqual(result["ingest"]["status"], "accepted")
            self.assertEqual(result["session_commit"], {})

        stop, _ = self.run_hook(
            {"message": "The session is complete; commit useful memory."},
            event="Stop",
            event_log=event_log,
        )
        self.assertEqual(stop["status"], "ok")
        self.assertEqual(stop["session_commit"]["status"], "committed")
        self.assertFalse(stop["session_commit"]["raw_events_duplicated"])
        self.assertGreaterEqual(stop["session_commit"]["segments_written"], 3)
        self.assertGreaterEqual(stop["session_commit"]["entities_written"], 3)

        records = [json.loads(line) for line in event_log.read_text().splitlines() if line.strip()]
        raw_events = [record for record in records if record.get("record_type") == "context_event"]
        self.assertEqual(len(raw_events), 5)
        topics = {record.get("topic") for record in records if record.get("record_type") == "context_segment"}
        self.assertIn("location", topics)
        self.assertIn("preference", topics)
        self.assertIn("approval_budget", topics)
        self.assertTrue(
            any(record.get("record_type") == "context_embedding" and record.get("embedding_type") == "node_l0" for record in records)
        )
        self.assertTrue(
            any(record.get("record_type") == "context_embedding" and record.get("embedding_type") == "node_l1" for record in records)
        )
        self.assertTrue(
            all(record.get("source_event_ids") for record in records if record.get("record_type") in {"context_segment", "context_entity"})
        )

        query, _ = self.run_hook(
            {"prompt": "Where am I currently located?"},
            event="UserPromptSubmit",
            event_log=event_log,
            query="Where am I currently located?",
        )
        self.assertGreaterEqual(query["retrieve"]["selected_ref_count"], 1)

    def test_stop_event_is_ingested_as_assistant_feedback_signal(self):
        event_log = Path(self.tmpdir.name) / "codex-hook.jsonl"
        result, _ = self.run_hook(
            {"message": "The final answer explained that Bob owns delivery."},
            event="Stop",
            event_log=event_log,
        )
        self.assertEqual(result["status"], "ok")
        records = [json.loads(line) for line in event_log.read_text().splitlines() if line.strip()]
        event = next(record for record in records if record.get("record_type") == "context_event")
        self.assertIn("assistant:", event["text"])
        self.assertEqual(event["agent_hook"]["hook_type"], "after_llm")


if __name__ == "__main__":
    unittest.main()
