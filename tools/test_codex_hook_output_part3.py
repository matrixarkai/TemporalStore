# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""_CodexHookOutputPart3 methods split from test_matrixark_codex_hook_output.MatrixArkCodexHookOutputTest (mixin)."""
from __future__ import annotations

from unittest import mock

try:  # package path
    from tools.matrixark_mcp_core import *  # noqa: F401,F403
except ImportError:
    from matrixark_mcp_core import *  # noqa: F401,F403

try:  # names owned by the parent module
    from tools.test_matrixark_codex_hook_output import (
    MatrixArkLocalAdapter,
    Namespace,
    Path,
    hook,
    matrixark_http,
    os,
    sys,
    tempfile,
)
except ImportError:
    from test_matrixark_codex_hook_output import (
    MatrixArkLocalAdapter,
    Namespace,
    Path,
    hook,
    matrixark_http,
    os,
    sys,
    tempfile,
)


class _CodexHookOutputPart3:
    def test_fast_async_hook_ingest_uses_local_tenant_fallback_scope(self) -> None:
        class Adapter:
            def __init__(self) -> None:
                self.raw_records = []
                self.serving_records = []
                self.session_buffer_records = []

            def enqueue_raw_ingestion_records(self, records):
                self.raw_records.extend(records)

            def _enqueue_direct_write(self, records):
                self.serving_records.extend(records)

            def append_session_buffer_event(self, **kwargs):
                self.session_buffer_records.append(kwargs)

            def pending_session_events(self, scope):
                return []

        class Server:
            def __init__(self) -> None:
                self.adapter = Adapter()

        args = Namespace(
            event="UserPromptSubmit",
            account_id="acct_hook_local",
            tenant_id="",
            user_id="local_user",
            session_id="codex-session-local",
            team="codex",
            project="temporalstore",
            session_commit_threshold=20,
            idle_commit_timeout_ms=0,
            understanding_provider="rules",
            segment_provider="deterministic",
        )
        server = Server()
        result = hook.fast_async_hook_ingest(
            server,
            args=args,
            text="Remember that live hook scope must use the local tenant fallback.",
            role="user",
            agent_context={"workspace_root": "/repo"},
            hook={"session_id_source": "payload_field", "thread_id": "thread-local-scope"},
        )

        self.assertEqual("accepted", result["status"])
        raw_record = server.adapter.raw_records[0]
        serving_event = next(record for record in server.adapter.serving_records if record["record_type"] == "context_event")
        buffered = server.adapter.session_buffer_records[0]
        for record in [raw_record, serving_event, buffered["envelope"]]:
            self.assertEqual("tenant_local_agent", record["scope"]["tenant_id"])
            self.assertEqual("local_user", record["scope"]["user_id"])
            self.assertEqual("codex-session-local", record["scope"]["session_id"])
        self.assertEqual(
            ["tenant:tenant_local_agent", "user:local_user", "session:codex-session-local", "conversation:codex_hook"],
            serving_event["node_path"],
        )
        self.assertEqual(serving_event["node_path"], buffered["node_path"])
        self.assertNotEqual("tenant:", serving_event["node_path"][0])

    def test_fast_async_hook_ingest_uses_local_user_fallback_for_profile_scope(self) -> None:
        class Adapter:
            def __init__(self) -> None:
                self.raw_records = []
                self.serving_records = []
                self.session_buffer_records = []

            def enqueue_raw_ingestion_records(self, records):
                self.raw_records.extend(records)

            def _enqueue_direct_write(self, records):
                self.serving_records.extend(records)

            def append_session_buffer_event(self, **kwargs):
                self.session_buffer_records.append(kwargs)

            def pending_session_events(self, scope):
                return []

        class Server:
            def __init__(self) -> None:
                self.adapter = Adapter()

        old_user = os.environ.get("MATRIXARK_LOCAL_USER_ID")
        os.environ["MATRIXARK_LOCAL_USER_ID"] = "codex_profile_user"
        try:
            args = Namespace(
                event="UserPromptSubmit",
                account_id="acct_hook_local",
                tenant_id="",
                user_id="",
                session_id="",
                team="codex",
                project="temporalstore",
                session_commit_threshold=20,
                idle_commit_timeout_ms=0,
                understanding_provider="rules",
                segment_provider="deterministic",
            )
            server = Server()
            result = hook.fast_async_hook_ingest(
                server,
                args=args,
                text="Remember that local Codex hook memory should promote into the user profile.",
                role="user",
                agent_context={"workspace_root": "/repo"},
                hook={"session_id_source": "payload_field", "thread_id": "thread-profile-scope"},
            )
        finally:
            if old_user is None:
                os.environ.pop("MATRIXARK_LOCAL_USER_ID", None)
            else:
                os.environ["MATRIXARK_LOCAL_USER_ID"] = old_user

        self.assertEqual("accepted", result["status"])
        serving_event = next(record for record in server.adapter.serving_records if record["record_type"] == "context_event")
        buffered = server.adapter.session_buffer_records[0]
        for record in [server.adapter.raw_records[0], serving_event, buffered["envelope"]]:
            self.assertEqual("codex_profile_user", record["scope"]["user_id"])
            self.assertEqual("codex_profile_user", record["scope"]["session_id"])
        self.assertEqual(
            ["tenant:tenant_local_agent", "user:codex_profile_user", "session:codex_profile_user", "conversation:codex_hook"],
            serving_event["node_path"],
        )
        self.assertEqual(serving_event["node_path"], buffered["node_path"])

    def test_cli_user_id_default_uses_local_profile_identity(self) -> None:
        old_argv = sys.argv
        old_user = os.environ.get("MATRIXARK_USER_ID")
        old_local_user = os.environ.get("MATRIXARK_LOCAL_USER_ID")
        os.environ.pop("MATRIXARK_USER_ID", None)
        os.environ["MATRIXARK_LOCAL_USER_ID"] = "codex_cli_profile_user"
        sys.argv = ["matrixark_codex_hook.py"]
        try:
            args = hook.parse_args()
        finally:
            sys.argv = old_argv
            if old_user is None:
                os.environ.pop("MATRIXARK_USER_ID", None)
            else:
                os.environ["MATRIXARK_USER_ID"] = old_user
            if old_local_user is None:
                os.environ.pop("MATRIXARK_LOCAL_USER_ID", None)
            else:
                os.environ["MATRIXARK_LOCAL_USER_ID"] = old_local_user

        self.assertEqual("codex_cli_profile_user", args.user_id)

    def test_cli_auto_batch_idle_timeout_defaults_to_live_flush(self) -> None:
        old_argv = sys.argv
        old_timeout = os.environ.get("MATRIXARK_IDLE_COMMIT_TIMEOUT_MS")
        os.environ.pop("MATRIXARK_IDLE_COMMIT_TIMEOUT_MS", None)
        sys.argv = ["matrixark_codex_hook.py"]
        try:
            args = hook.parse_args()
        finally:
            sys.argv = old_argv
            if old_timeout is None:
                os.environ.pop("MATRIXARK_IDLE_COMMIT_TIMEOUT_MS", None)
            else:
                os.environ["MATRIXARK_IDLE_COMMIT_TIMEOUT_MS"] = old_timeout

        self.assertEqual(hook.DEFAULT_IDLE_COMMIT_TIMEOUT_MS, args.idle_commit_timeout_ms)
        ingest_args = hook.apply_hook_auto_batch_ingest_options(
            {},
            event="UserPromptSubmit",
            session_commit_threshold=args.session_commit_threshold,
            idle_commit_timeout_ms=args.idle_commit_timeout_ms,
        )
        self.assertTrue(ingest_args["auto_batch_extract"])
        self.assertEqual(hook.DEFAULT_IDLE_COMMIT_TIMEOUT_MS, ingest_args["idle_commit_timeout_ms"])

    def test_cli_explicit_zero_idle_timeout_disables_live_flush(self) -> None:
        ingest_args = hook.apply_hook_auto_batch_ingest_options(
            {"idle_commit_timeout_ms": 123},
            event="UserPromptSubmit",
            session_commit_threshold=20,
            idle_commit_timeout_ms=0,
        )

        self.assertTrue(ingest_args["auto_batch_extract"])
        self.assertNotIn("idle_commit_timeout_ms", ingest_args)

    def test_dual_hook_uses_local_user_identity_without_hardcoded_user(self) -> None:
        source = (Path(__file__).resolve().parents[1] / "tools" / "matrixark_codex_dual_hook.sh").read_text()
        self.assertIn("HOOK_USER_ID=", source)
        self.assertIn("--user-id \"$HOOK_USER_ID\"", source)
        self.assertNotIn("--user-id \"${MATRIXARK_HOOK_USER_ID:-local_user}\"", source)


    # Keeping the raw tool blob is opt-in and this test reads raw_records[0] to assert the
    # RAW record shape, so without it the test dies on an empty list rather than on
    # anything it is about. The switch is read into a module constant at import, so the
    # environment cannot reach it from here -- patch the constant. Default behaviour is
    # covered by test_fast_async_tool_result_defaults_to_serving_memory.
    @mock.patch.object(hook, "HOOK_TOOL_RESULT_RAW", True)
    def test_fast_async_hook_ingest_marks_tool_evidence_lifecycle(self) -> None:
        class Adapter:
            def __init__(self) -> None:
                self.raw_records = []
                self.serving_records = []
                self.session_buffer_records = []

            def enqueue_raw_ingestion_records(self, records):
                self.raw_records.extend(records)

            def _enqueue_direct_write(self, records):
                self.serving_records.extend(records)

            def append_session_buffer_event(self, **kwargs):
                self.session_buffer_records.append(kwargs)

            def pending_session_events(self, scope):
                return [{"event_id_hash": 1}]

        class Server:
            def __init__(self) -> None:
                self.adapter = Adapter()

        args = Namespace(
            event="PostToolUse",
            account_id="acct_local",
            tenant_id="tenant_codex",
            user_id="local_user",
            session_id="codex-session-tool",
            team="codex",
            project="temporalstore",
            session_commit_threshold=20,
            idle_commit_timeout_ms=0,
            understanding_provider="rules",
            segment_provider="deterministic",
        )
        server = Server()
        hook.fast_async_hook_ingest(
            server,
            args=args,
            text="Exit code: 0\nRan 77 tests in 2.4s\nOK",
            role="tool",
            agent_context={"workspace_root": "/repo"},
            hook={"session_id_source": "payload_field"},
        )

        raw_record = server.adapter.raw_records[0]
        self.assertEqual("tool", raw_record["source_role"])
        self.assertEqual("tool", raw_record["role"])
        self.assertEqual("PostToolUse", raw_record["codex_api_event"])
        self.assertNotIn("hook_type", raw_record)
        self.assertNotIn("hook_type", raw_record.get("agent_hook", {}))
        self.assertEqual("PostToolUse", raw_record["codex_event"])
        serving_event = next(record for record in server.adapter.serving_records if record["record_type"] == "context_event")
        self.assertEqual("tool", serving_event["source_role"])
        self.assertEqual("tool", serving_event["role"])
        self.assertEqual("PostToolUse", serving_event["codex_api_event"])
        self.assertNotIn("hook_type", serving_event)
        self.assertEqual(["tool_result"], serving_event["source_hook_types"])
        self.assertEqual("PostToolUse", serving_event["codex_event"])
        self.assertEqual("tool", serving_event["envelope"]["source_role"])
        self.assertNotIn("hook_type", serving_event["envelope"])

    def test_fast_async_hook_ingest_flushes_serving_projection_before_hook_exit(self) -> None:
        class Adapter:
            def __init__(self) -> None:
                self.raw_records = []
                self.queued_records = []
                self.materialized_records = []
                self.materialized_batches = []
                self.session_buffer_records = []

            def enqueue_raw_ingestion_records(self, records):
                self.raw_records.extend(records)

            def _enqueue_direct_write(self, records):
                self.queued_records.extend(records)

            def _append_many_materialized(self, records, *, allow_queue=True):
                self.materialized_batches.append(list(records))
                self.materialized_records.extend(records)

            def append_session_buffer_event(self, **kwargs):
                self.session_buffer_records.append(kwargs)

            def pending_session_events(self, scope):
                return []

        class Server:
            def __init__(self) -> None:
                self.adapter = Adapter()

        args = Namespace(
            event="Stop",
            account_id="acct_local",
            tenant_id="tenant_codex",
            user_id="local_user",
            session_id="codex-session-sync-serving",
            team="codex",
            project="temporalstore",
            session_commit_threshold=20,
            idle_commit_timeout_ms=0,
            understanding_provider="rules",
            segment_provider="deterministic",
        )
        server = Server()
        hook.fast_async_hook_ingest(
            server,
            args=args,
            text="Final assistant answer must be visible in serving before hook exit.",
            role="assistant",
            agent_context={"workspace_root": "/repo"},
            hook={"session_id_source": "payload_field"},
        )

        self.assertEqual([], server.adapter.queued_records)
        self.assertEqual("context_event", server.adapter.materialized_batches[0][0]["record_type"])
        serving_event = next(
            record for record in server.adapter.materialized_records if record["record_type"] == "context_event"
        )
        self.assertEqual("assistant", serving_event["role"])
        self.assertEqual("Stop", serving_event["codex_api_event"])
        self.assertNotIn("hook_type", serving_event)
        self.assertEqual(["after_llm"], serving_event["source_hook_types"])
        self.assertIn("assistant: Final assistant answer", serving_event["text"])

    # Keeping the raw tool blob is opt-in and this test reads raw_records[0] to assert the
    # RAW record shape, so without it the test dies on an empty list rather than on
    # anything it is about. The switch is read into a module constant at import, so the
    # environment cannot reach it from here -- patch the constant. Default behaviour is
    # covered by test_fast_async_tool_result_defaults_to_serving_memory.
    @mock.patch.object(hook, "HOOK_TOOL_RESULT_RAW", True)
    def test_fast_async_hook_ingest_threshold_commits_tool_evidence(self) -> None:
        original_auto_batch = hook.HOOK_AUTO_BATCH_EXTRACT
        hook.HOOK_AUTO_BATCH_EXTRACT = True

        class Adapter:
            def __init__(self) -> None:
                self.raw_records = []
                self.serving_records = []
                self.session_buffer_records = []
                self.commit_calls = []

            def enqueue_raw_ingestion_records(self, records):
                self.raw_records.extend(records)

            def _enqueue_direct_write(self, records):
                self.serving_records.extend(records)

            def append_session_buffer_event(self, **kwargs):
                self.session_buffer_records.append(kwargs)

            def pending_session_events(self, scope):
                return [{"event_id_hash": 1}, {"event_id_hash": 2}]

            def session_commit(self, args, *, hook=None):
                self.commit_calls.append((args, hook))
                return {
                    "status": "committed",
                    "trigger_policy": "threshold",
                    "extraction_phase": "provisional",
                    "final_session_boundary": False,
                    "committed_event_count": 2,
                    "entities_written": 1,
                    "profile_entities_written": 1,
                    "indexes_written": 2,
                }

        class Server:
            def __init__(self) -> None:
                self.adapter = Adapter()

        try:
            args = Namespace(
                event="PostToolUse",
                account_id="acct_local",
                tenant_id="tenant_codex",
                user_id="local_user",
                session_id="codex-session-tool-threshold",
                team="codex",
                project="temporalstore",
                session_commit_threshold=2,
                idle_commit_timeout_ms=0,
                understanding_provider="rules",
                extraction_provider="rules",
                segment_provider="deterministic",
                segment_model="codex-memory-tool-segmenter",
                segment_model_path="/models/codex-memory-tool-segmenter",
                segment_max_new_tokens=96,
                segment_provider_fallback="rules",
                skip_prior_context=True,
            )
            server = Server()
            result = hook.fast_async_hook_ingest(
                server,
                args=args,
                text="Exit code: 0\nRan 81 tests in 1.2s\nOK",
                role="tool",
                agent_context={"workspace_root": "/repo"},
                hook={"session_id_source": "payload_field", "thread_id": "thread-tool-threshold"},
            )
        finally:
            hook.HOOK_AUTO_BATCH_EXTRACT = original_auto_batch

        self.assertEqual("committed", result["auto_batch_extract_result"]["status"])
        self.assertEqual("threshold", result["auto_batch_extract_result"]["trigger_policy"])
        self.assertEqual(2, result["session_buffer"]["pending_before_ingest_count"])
        self.assertEqual(2, result["session_buffer"]["pending_after_ingest_count"])
        self.assertTrue(result["session_buffer"]["commit_after_current_ingest"])
        self.assertEqual(1, len(server.adapter.commit_calls))
        commit_args, commit_hook = server.adapter.commit_calls[0]
        self.assertEqual("threshold", commit_args["commit_reason"])
        self.assertFalse(commit_args["force"])
        self.assertEqual(2, commit_args["max_messages"])
        self.assertEqual("rules", commit_args["understanding_provider"])
        self.assertEqual("rules", commit_args["extraction_provider"])
        self.assertEqual("deterministic", commit_args["segment_provider"])
        self.assertEqual("codex-memory-tool-segmenter", commit_args["segment_model"])
        self.assertEqual("/models/codex-memory-tool-segmenter", commit_args["segment_model_path"])
        self.assertEqual(96, commit_args["segment_max_new_tokens"])
        self.assertEqual("rules", commit_args["segment_provider_fallback"])
        self.assertTrue(commit_args["skip_prior_context"])
        self.assertEqual("session_commit", commit_hook["hook_type"])
        self.assertNotIn("thread_id", commit_hook)
        self.assertEqual(1, len(server.adapter.session_buffer_records))
        self.assertEqual("tool", server.adapter.session_buffer_records[0]["envelope"]["messages"][0]["role"])
        raw_selection = server.adapter.raw_records[0]["metadata"]["codex_memory_selection"]
        self.assertEqual("selected_tool_evidence_only", raw_selection["policy"])
        self.assertFalse(raw_selection["large_payload_verbatim_stored"])
        event_records = [record for record in server.adapter.serving_records if record.get("record_type") == "context_event"]
        self.assertEqual("selected_tool_evidence_only", event_records[0]["codex_memory_selection"]["policy"])

    def test_fast_async_hook_ingest_schedules_threshold_task_when_commit_api_missing(self) -> None:
        original_auto_batch = hook.HOOK_AUTO_BATCH_EXTRACT
        hook.HOOK_AUTO_BATCH_EXTRACT = True

        class Adapter:
            def __init__(self) -> None:
                self.raw_records = []
                self.serving_records = []
                self.session_buffer_records = []

            def enqueue_raw_ingestion_records(self, records):
                self.raw_records.extend(records)

            def _enqueue_direct_write(self, records):
                self.serving_records.extend(records)

            def append_session_buffer_event(self, **kwargs):
                self.session_buffer_records.append(kwargs)

            def pending_session_events(self, scope):
                return [{"event_id_hash": 1}, {"event_id_hash": 2}]

        class Server:
            def __init__(self) -> None:
                self.adapter = Adapter()

        try:
            args = Namespace(
                event="UserPromptSubmit",
                account_id="acct_local",
                tenant_id="tenant_codex",
                user_id="local_user",
                session_id="codex-session-threshold-task",
                team="codex",
                project="temporalstore",
                session_commit_threshold=2,
                idle_commit_timeout_ms=300000,
                understanding_provider="rules",
                segment_provider="deterministic",
            )
            server = Server()
            result = hook.fast_async_hook_ingest(
                server,
                args=args,
                text="We should remember that threshold extraction must still run without a sync commit API.",
                role="user",
                agent_context={"workspace_root": "/repo"},
                hook={"session_id_source": "payload_field"},
            )
        finally:
            hook.HOOK_AUTO_BATCH_EXTRACT = original_auto_batch

        self.assertEqual("deferred", result["auto_batch_extract_result"]["status"])
        self.assertEqual("threshold", result["auto_batch_extract_result"]["trigger_policy"])
        self.assertTrue(result["auto_batch_extract_result"]["threshold_commit_scheduled"])
        self.assertTrue(result["session_buffer"]["threshold_ready"])
        self.assertTrue(result["session_buffer"]["threshold_commit_scheduled"])
        threshold_tasks = [
            record
            for record in server.adapter.serving_records
            if record.get("record_type") == "matrixark_async_pipeline_task"
            and record.get("status") == "threshold_commit_scheduled"
        ]
        self.assertEqual(1, len(threshold_tasks))
        task = threshold_tasks[0]
        self.assertEqual("session_buffer_threshold_reached", task["reason"])
        self.assertEqual("threshold", task["trigger_policy"])
        self.assertEqual(2, task["threshold_messages"])
        self.assertEqual(2, task["threshold_pending_event_count"])
        self.assertEqual("provisional", task["extraction_phase"])
        self.assertFalse(task["final_session_boundary"])

    def test_fast_async_hook_ingest_preflushes_idle_tail_before_tool_evidence(self) -> None:
        original_auto_batch = hook.HOOK_AUTO_BATCH_EXTRACT
        hook.HOOK_AUTO_BATCH_EXTRACT = True

        class Adapter:
            def __init__(self) -> None:
                self.raw_records = []
                self.serving_records = []
                self.session_buffer_records = []
                self.commit_calls = []
                self.pending = [{"event_id_hash": 91, "envelope": {"ingestion_time_ms": 1}}]

            def enqueue_raw_ingestion_records(self, records):
                self.raw_records.extend(records)

            def _enqueue_direct_write(self, records):
                self.serving_records.extend(records)

            def append_session_buffer_event(self, **kwargs):
                self.session_buffer_records.append(kwargs)
                self.pending.append({"event_id_hash": kwargs["event_id_hash"], "envelope": kwargs["envelope"]})

            def pending_session_events(self, scope):
                return list(self.pending)

            def session_commit(self, args, *, hook=None):
                self.commit_calls.append((args, hook))
                committed = list(self.pending)
                self.pending.clear()
                return {
                    "status": "committed",
                    "trigger_policy": "idle_timeout",
                    "extraction_phase": "provisional",
                    "final_session_boundary": False,
                    "committed_event_count": len(committed),
                    "source_event_ids": [record["event_id_hash"] for record in committed],
                    "entities_written": 1,
                    "profile_entities_written": 1,
                    "indexes_written": 1,
                }

        class Server:
            def __init__(self) -> None:
                self.adapter = Adapter()

        try:
            args = Namespace(
                event="PostToolUse",
                account_id="acct_local",
                tenant_id="tenant_codex",
                user_id="local_user",
                session_id="codex-session-tool-idle",
                team="codex",
                project="temporalstore",
                session_commit_threshold=20,
                idle_commit_timeout_ms=1,
                understanding_provider="rules",
                segment_provider="deterministic",
            )
            server = Server()
            result = hook.fast_async_hook_ingest(
                server,
                args=args,
                text="Exit code: 0\nTool evidence arrived after an idle tail.",
                role="tool",
                agent_context={"workspace_root": "/repo"},
                hook={"session_id_source": "payload_field", "thread_id": "thread-tool-idle"},
            )
        finally:
            hook.HOOK_AUTO_BATCH_EXTRACT = original_auto_batch

        self.assertEqual("committed", result["idle_commit_result"]["status"])
        self.assertEqual("idle_timeout", result["idle_commit_result"]["trigger_policy"])
        self.assertEqual([91], result["idle_commit_result"]["source_event_ids"])
        self.assertTrue(result["session_buffer"]["pre_ingest_idle_ready"])
        self.assertEqual(1, result["session_buffer"]["pending_before_ingest_count"])
        self.assertEqual(1, result["session_buffer"]["pending_after_ingest_count"])
        self.assertFalse(result["session_buffer"]["commit_after_current_ingest"])
        self.assertTrue(result["session_buffer"]["auto_batch_extract"])
        self.assertEqual(result["idle_commit_result"], result["auto_batch_extract_result"])
        self.assertEqual(1, len(server.adapter.commit_calls))
        commit_args, commit_hook = server.adapter.commit_calls[0]
        self.assertEqual("idle_timeout", commit_args["commit_reason"])
        self.assertFalse(commit_args["force"])
        self.assertEqual(1, commit_args["idle_timeout_ms"])
        self.assertEqual("idle_timeout_before_ingest", commit_hook["trigger"])
        self.assertEqual(1, len(server.adapter.session_buffer_records))
        self.assertEqual("tool", server.adapter.session_buffer_records[0]["envelope"]["messages"][0]["role"])

    def test_fast_async_hook_ingest_commits_idle_timeout_with_zero_timeout(self) -> None:
        class Adapter:
            def __init__(self) -> None:
                self.raw_records = []
                self.serving_records = []
                self.session_buffer_records = []
                self.commit_calls = []

            def enqueue_raw_ingestion_records(self, records):
                self.raw_records.extend(records)

            def _enqueue_direct_write(self, records):
                self.serving_records.extend(records)

            def append_session_buffer_event(self, **kwargs):
                self.session_buffer_records.append(kwargs)

            def pending_session_events(self, scope):
                return [{"event_id_hash": 1}]

            def session_commit(self, args, *, hook=None):
                self.commit_calls.append((args, hook))
                return {
                    "status": "committed",
                    "trigger_policy": "idle_timeout",
                    "extraction_phase": "provisional",
                    "final_session_boundary": False,
                    "committed_event_count": 1,
                    "extraction_context_event_count": 2,
                    "entities_written": 3,
                    "profile_entities_written": 1,
                    "trigger_evidence": {
                        "pending_event_count": 1,
                        "threshold_messages": 20,
                        "threshold_ready": False,
                        "idle_timeout_ms": 0,
                        "idle_ready": True,
                        "force": False,
                    },
                }

        class Server:
            def __init__(self) -> None:
                self.adapter = Adapter()

        args = Namespace(
            event="IdleTimeout",
            account_id="acct_local",
            tenant_id="tenant_codex",
            user_id="local_user",
            session_id="codex-session-idle",
            team="codex",
            project="temporalstore",
            session_commit_threshold=20,
            idle_commit_timeout_ms=0,
            understanding_provider="rules",
            segment_provider="deterministic",
        )
        server = Server()
        result = hook.fast_async_hook_ingest(
            server,
            args=args,
            text="idle tick",
            role="assistant",
            agent_context={"workspace_root": "/repo"},
            hook={"session_id_source": "payload_field"},
        )

        self.assertEqual("committed", result["session_commit"]["status"])
        self.assertEqual("idle_timeout", result["session_commit"]["trigger_policy"])
        self.assertEqual("provisional", result["session_commit"]["extraction_phase"])
        self.assertFalse(result["session_commit"]["final_session_boundary"])
        self.assertEqual(2, result["session_commit"]["extraction_context_event_count"])
        self.assertEqual(3, result["session_commit"]["memory_layers_written"]["session_entities"])
        self.assertEqual(1, result["session_commit"]["memory_layers_written"]["profile_entities"])
        self.assertEqual("provisional", result["session_commit"]["memory_layers_written"]["extraction_phase"])
        self.assertTrue(result["session_commit"]["trigger_evidence"]["idle_ready"])
        self.assertEqual(1, len(server.adapter.commit_calls))
        commit_args, _commit_hook = server.adapter.commit_calls[0]
        self.assertEqual("idle_timeout", commit_args["commit_reason"])
        self.assertFalse(commit_args["force"])
        self.assertEqual(0, commit_args["idle_timeout_ms"])
        self.assertNotIn("max_messages", commit_args)

    def test_fast_async_hook_ingest_stop_boundary_force_commits_assistant_response(self) -> None:
        class Adapter:
            def __init__(self) -> None:
                self.raw_records = []
                self.serving_records = []
                self.session_buffer_records = []
                self.commit_calls = []

            def enqueue_raw_ingestion_records(self, records):
                self.raw_records.extend(records)

            def _enqueue_direct_write(self, records):
                self.serving_records.extend(records)

            def append_session_buffer_event(self, **kwargs):
                self.session_buffer_records.append(kwargs)

            def pending_session_events(self, scope):
                return [{"event_id_hash": 1}, {"event_id_hash": 2}, {"event_id_hash": 3}]

            def session_commit(self, args, *, hook=None):
                self.commit_calls.append((args, hook))
                return {
                    "status": "committed",
                    "trigger_policy": "force",
                    "extraction_phase": "final",
                    "final_session_boundary": True,
                    "committed_event_count": 3,
                    "entities_written": 4,
                    "profile_entities_written": 2,
                    "indexes_written": 6,
                }

        class Server:
            def __init__(self) -> None:
                self.adapter = Adapter()

        args = Namespace(
            event="Stop",
            account_id="acct_local",
            tenant_id="tenant_codex",
            user_id="local_user",
            session_id="codex-session-stop",
            team="codex",
            project="temporalstore",
            session_commit_threshold=20,
            idle_commit_timeout_ms=0,
            understanding_provider="rules",
            segment_provider="deterministic",
        )
        server = Server()
        result = hook.fast_async_hook_ingest(
            server,
            args=args,
            text="Done. The hook now commits the final assistant decision into profile memory.",
            role="assistant",
            agent_context={"workspace_root": "/repo"},
            hook={"session_id_source": "payload_field", "thread_id": "thread-stop"},
        )

        self.assertEqual("accepted", result["status"])
        self.assertTrue(result["session_buffer"]["boundary_commit_requested"])
        self.assertFalse(result["session_buffer"]["threshold_ready"])
        self.assertEqual("committed", result["session_commit"]["status"])
        self.assertEqual("force", result["session_commit"]["trigger_policy"])
        self.assertEqual("final", result["session_commit"]["extraction_phase"])
        self.assertTrue(result["session_commit"]["final_session_boundary"])
        self.assertEqual(2, result["session_commit"]["profile_entities_written"])
        self.assertEqual(2, result["session_commit"]["memory_layers_written"]["profile_entities"])
        self.assertEqual(1, len(server.adapter.commit_calls))
        commit_args, commit_hook = server.adapter.commit_calls[0]
        self.assertEqual("hook_boundary", commit_args["commit_reason"])
        self.assertTrue(commit_args["force"])
        self.assertNotIn("max_messages", commit_args)
        self.assertEqual("session_commit", commit_hook["hook_type"])
        self.assertEqual("Stop", commit_hook["trigger"])
        self.assertNotIn("thread_id", commit_hook)
        self.assertEqual(1, len(server.adapter.session_buffer_records))
        buffered = server.adapter.session_buffer_records[0]
        self.assertEqual("assistant", buffered["envelope"]["messages"][0]["role"])
        self.assertNotIn("hook_type", buffered["envelope"])
        self.assertEqual("Stop", buffered["envelope"]["codex_event"])
        selection = buffered["envelope"]["codex_memory_selection"]
        self.assertEqual("selected_assistant_decision_outcome_only", selection["policy"])
        self.assertFalse(selection["large_payload_verbatim_stored"])

    def test_fast_async_hook_stop_boundary_surfaces_finalized_drained_session(self) -> None:
        class Adapter:
            def __init__(self) -> None:
                self.raw_records = []
                self.serving_records = []
                self.session_buffer_records = []
                self.commit_calls = []

            def enqueue_raw_ingestion_records(self, records):
                self.raw_records.extend(records)

            def _enqueue_direct_write(self, records):
                self.serving_records.extend(records)

            def append_session_buffer_event(self, **kwargs):
                self.session_buffer_records.append(kwargs)

            def pending_session_events(self, scope):
                return []

            def session_commit(self, args, *, hook=None):
                self.commit_calls.append((args, hook))
                return {
                    "status": "finalized",
                    "trigger_policy": "force",
                    "commit_reason": "hook_boundary",
                    "extraction_phase": "final",
                    "final_session_boundary": True,
                    "prior_commit_count": 1,
                    "prior_committed_event_count": 2,
                    "boundary_hash": 909,
                    "summary_refresh": {"status": "dirty_marked", "dirty_reason": "session_finalized"},
                    "trigger_evidence": {
                        "force": True,
                        "pending_event_count": 0,
                        "already_finalized": False,
                    },
                }

        class Server:
            def __init__(self) -> None:
                self.adapter = Adapter()

        args = Namespace(
            event="Stop",
            account_id="acct_local",
            tenant_id="tenant_codex",
            user_id="local_user",
            session_id="codex-session-stop-finalized",
            team="codex",
            project="temporalstore",
            session_commit_threshold=20,
            idle_commit_timeout_ms=0,
            understanding_provider="rules",
            extraction_provider="rules",
            segment_provider="deterministic",
        )
        server = Server()
        result = hook.fast_async_hook_ingest(
            server,
            args=args,
            text="Final assistant boundary after threshold-drained session.",
            role="assistant",
            agent_context={"workspace_root": "/repo"},
            hook={"session_id_source": "payload_field"},
        )

        self.assertEqual("finalized", result["session_commit"]["status"])
        self.assertEqual(result["session_commit"], result["auto_batch_extract_result"])
        decision = hook.auto_batch_decision_summary(result)
        self.assertEqual("boundary_commit", decision["decision"])
        self.assertEqual("finalized", decision["auto_batch_extract_status"])
        self.assertTrue(decision["final_session_boundary"])
        commit_summary = hook.session_commit_summary(result["session_commit"])
        self.assertEqual(1, commit_summary["prior_commit_count"])
        self.assertEqual(2, commit_summary["prior_committed_event_count"])
        self.assertEqual(909, commit_summary["boundary_hash"])
        self.assertEqual("dirty_marked", decision["summary_refresh"]["status"])
        self.assertEqual(1, len(server.adapter.commit_calls))

    def test_fast_async_tool_result_defaults_to_serving_memory(self) -> None:
        class Adapter:
            def __init__(self) -> None:
                self.raw_records = []
                self.serving_records = []

            def enqueue_raw_ingestion_records(self, records):
                self.raw_records.extend(records)

            def _enqueue_direct_write(self, records):
                self.serving_records.extend(records)

        class Server:
            def __init__(self) -> None:
                self.adapter = Adapter()

        args = Namespace(
            event="PostToolUse",
            account_id="acct_local",
            tenant_id="tenant_codex",
            user_id="local_user",
            session_id="codex-native-session-1",
            team="codex",
            project="temporalstore",
            session_commit_threshold=20,
            idle_commit_timeout_ms=0,
            understanding_provider="rules",
            segment_provider="deterministic",
        )

        server = Server()
        result = hook.fast_async_hook_ingest(
            server,
            args=args,
            text="Exit code: 0; Ran 9 tests",
            role="tool",
            agent_context={"workspace_root": "/repo", "tool_name": "shell_command"},
            hook={"session_id_source": "payload_field"},
            original_text="verbose stdout\nExit code: 0\nRan 9 tests\nOK",
        )

        # Keeping the raw tool blob is OPT-IN: HOOK_TOOL_RESULT_RAW has defaulted off since it
        # was introduced, and its fields were once even named tool_result_raw_opt_in_env. What
        # a tool result leaves behind by default is the selected serving projection, which is
        # what this test is named for. The opt-in itself is covered by
        # test_fast_async_tool_result_raw_override_keeps_raw_only.
        self.assertEqual("accepted", result["status"])
        self.assertEqual("skipped_tool_result_raw_capture", result["raw_ingestion_status"])
        self.assertEqual("accepted", result["serving_projection_status"])
        self.assertEqual("pending", result["async_pipeline_status"])
        self.assertEqual([], server.adapter.raw_records)
        self.assertGreaterEqual(len(server.adapter.serving_records), 1)
        serving = next(record for record in server.adapter.serving_records if record["record_type"] == "context_event")
        self.assertEqual("tool", serving["role"])
        self.assertEqual("serving", serving["metadata"]["serving_projection"]["visibility"])
        # The projection is the SELECTED text, not the raw stdout it was taken from.
        self.assertIn("Ran 9 tests", serving["text"])
        self.assertNotIn("verbose stdout", serving["text"])

    def test_fast_async_tool_result_raw_override_keeps_raw_only(self) -> None:
        class Adapter:
            def __init__(self) -> None:
                self.raw_records = []
                self.serving_records = []

            def enqueue_raw_ingestion_records(self, records):
                self.raw_records.extend(records)

            def _enqueue_direct_write(self, records):
                self.serving_records.extend(records)

        class Server:
            def __init__(self) -> None:
                self.adapter = Adapter()

        args = Namespace(
            event="PostToolUse",
            account_id="acct_local",
            tenant_id="tenant_codex",
            user_id="local_user",
            session_id="codex-native-session-1",
            team="codex",
            project="temporalstore",
            session_commit_threshold=20,
            idle_commit_timeout_ms=0,
            understanding_provider="rules",
            segment_provider="deterministic",
        )

        original_raw = hook.HOOK_TOOL_RESULT_RAW
        original_serving = hook.HOOK_TOOL_RESULT_SERVING
        hook.HOOK_TOOL_RESULT_RAW = True
        hook.HOOK_TOOL_RESULT_SERVING = False
        try:
            server = Server()
            result = hook.fast_async_hook_ingest(
                server,
                args=args,
                text="Exit code: 0; Ran 9 tests",
                role="tool",
                agent_context={"workspace_root": "/repo", "tool_name": "shell_command"},
                hook={"session_id_source": "payload_field"},
                original_text="verbose stdout\nExit code: 0\nRan 9 tests\nOK",
            )
        finally:
            hook.HOOK_TOOL_RESULT_RAW = original_raw
            hook.HOOK_TOOL_RESULT_SERVING = original_serving

        self.assertEqual("accepted", result["status"])
        self.assertEqual("accepted", result["raw_ingestion_status"])
        self.assertEqual("skipped_raw_only_tool_result", result["serving_projection_status"])
        self.assertEqual(1, len(server.adapter.raw_records))
        self.assertEqual(0, len(server.adapter.serving_records))
        raw = server.adapter.raw_records[0]
        self.assertEqual("tool", raw["role"])
        self.assertEqual("raw_only", raw["metadata"]["serving_projection"]["visibility"])
        self.assertEqual("raw_only_compact_evidence", raw["metadata"]["tool_result_ingestion"]["policy"])

    def test_fast_async_tool_result_serving_override_promotes_context_event(self) -> None:
        class Adapter:
            def __init__(self) -> None:
                self.raw_records = []
                self.serving_records = []

            def enqueue_raw_ingestion_records(self, records):
                self.raw_records.extend(records)

            def _enqueue_direct_write(self, records):
                self.serving_records.extend(records)

        class Server:
            def __init__(self) -> None:
                self.adapter = Adapter()

        args = Namespace(
            event="PostToolUse",
            account_id="acct_local",
            tenant_id="tenant_codex",
            user_id="local_user",
            session_id="codex-native-session-1",
            team="codex",
            project="temporalstore",
            session_commit_threshold=20,
            idle_commit_timeout_ms=0,
            understanding_provider="rules",
            segment_provider="deterministic",
        )

        original = hook.HOOK_TOOL_RESULT_SERVING
        hook.HOOK_TOOL_RESULT_SERVING = True
        try:
            server = Server()
            result = hook.fast_async_hook_ingest(
                server,
                args=args,
                text="Exit code: 0; Ran 9 tests",
                role="tool",
                agent_context={"workspace_root": "/repo", "tool_name": "shell_command"},
                hook={"session_id_source": "payload_field"},
                original_text="verbose stdout\nExit code: 0\nRan 9 tests\nOK",
            )
        finally:
            hook.HOOK_TOOL_RESULT_SERVING = original

        self.assertEqual("accepted", result["status"])
        # Only HOOK_TOOL_RESULT_SERVING is pinned above; the raw opt-in is untouched, so no raw
        # record is kept. This test is about the serving projection being promoted.
        self.assertEqual([], server.adapter.raw_records)
        self.assertGreaterEqual(len(server.adapter.serving_records), 1)
        serving = next(record for record in server.adapter.serving_records if record["record_type"] == "context_event")
        self.assertEqual("tool", serving["role"])
        self.assertEqual("serving", serving["metadata"]["serving_projection"]["visibility"])

    def test_retention_keeps_acceptance_prompt_that_mentions_synthetic_rows(self) -> None:
        fields = hook.hook_retention_fields(
            text=(
                "Fix MatrixArk Codex realtime hook ingestion/query. "
                "Query top K with validation/probe/synthetic rows hidden by default."
            ),
            role="user",
            now_ms=123,
        )

        self.assertFalse(fields["synthetic"])
        self.assertEqual("normal", fields["retention_class"])

    def test_retention_marks_explicit_synthetic_probe_debug(self) -> None:
        fields = hook.hook_retention_fields(
            text="MatrixArk synthetic probe global cmd bash 1784781133",
            role="user",
            now_ms=123,
        )

        self.assertTrue(fields["synthetic"])
        self.assertEqual("debug", fields["retention_class"])

    def test_query_filter_keeps_natural_prompt_with_stale_synthetic_flag(self) -> None:
        text = (
            "Fix MatrixArk Codex realtime hook ingestion/query. "
            "Query top K with validation/probe/synthetic rows hidden by default."
        )

        self.assertTrue(
            matrixark_http._hook_is_real_user(
                {"synthetic": True, "record_type": "agent_message"},
                "user",
                text,
                real_user_only=True,
                include_synthetic=False,
            )
        )
        self.assertFalse(
            matrixark_http._hook_is_real_user(
                {"synthetic": True, "record_type": "agent_message"},
                "user",
                "MatrixArk synthetic probe global cmd bash 1784781133",
                real_user_only=True,
                include_synthetic=False,
            )
        )
        self.assertFalse(
            matrixark_http._hook_is_real_user(
                {"synthetic": False, "record_type": "agent_message"},
                "user",
                "manual validation loose payload parser row 1784778866123",
                real_user_only=True,
                include_synthetic=False,
            )
        )
        self.assertFalse(
            matrixark_http._hook_is_real_user(
                {"synthetic": False, "record_type": "agent_message"},
                "user",
                "You are a helpful assistant. You will be presented with a user prompt, and your job is to provide a short title for a task.",
                real_user_only=True,
                include_synthetic=False,
            )
        )
        self.assertFalse(
            matrixark_http._hook_is_real_user(
                {"synthetic": False, "record_type": "context_event"},
                "user",
                "user: You are a helpful assistant. You will be presented with a user prompt, and your job is to provide a short title for a task.",
                real_user_only=True,
                include_synthetic=False,
            )
        )
        self.assertFalse(
            matrixark_http._hook_is_real_user(
                {"synthetic": False, "record_type": "agent_message"},
                "user",
                "MatrixArk cmd wrapper direct smoke 1784771237422",
                real_user_only=True,
                include_synthetic=False,
            )
        )
        self.assertFalse(
            matrixark_http._hook_is_real_user(
                {"synthetic": False, "record_type": "agent_message"},
                "user",
                "matrixark plain string prompt hook proof 1784770203",
                real_user_only=True,
                include_synthetic=False,
            )
        )

    def test_hook_dedupe_prefers_raw_over_context_event_user_prefix(self) -> None:
        rows = matrixark_http._hook_dedupe_rows(
            [
                {
                    "backend": "native",
                    "session_id": "codex:session",
                    "text": "user: same prompt",
                    "timestamp_ms": 100,
                    "sequence": 1,
                    "projection": "records",
                },
                {
                    "backend": "native",
                    "session_id": "codex:session",
                    "text": "same prompt",
                    "timestamp_ms": 100,
                    "sequence": 2,
                    "projection": "raw_ingestion",
                },
            ]
        )

        self.assertEqual(1, len(rows))
        self.assertEqual("raw_ingestion", rows[0]["projection"])

    def test_dual_hook_has_no_persistent_hook_logs(self) -> None:
        script = (Path(__file__).resolve().parents[1] / "tools" / "matrixark_codex_dual_hook.sh").read_text()

        self.assertIn('NATIVE_HOOK_STDOUT="/dev/null"', script)
        self.assertIn('RUST_PUBLISH_STDERR="/dev/null"', script)
        self.assertIn('NATIVE_PUBLISH_STDERR="/dev/null"', script)
        self.assertIn('export MATRIXARK_CODEX_HOOK_DIAG_LOG=""', script)
        self.assertNotIn('MATRIXARK_CODEX_HOOK_LOG_DIR', script)
        self.assertNotIn('dispatch-diagnostics.jsonl', script)
        self.assertNotIn('with open(os.environ.get("MATRIXARK_CODEX_HOOK_DIAG_LOG"', script)
        self.assertNotIn('native-$EVENT.out', script)
        self.assertNotIn('rust-service-publish.err', script)
        self.assertNotIn('native-direct-publish.err', script)

    def test_python_hook_live_fast_path_defaults_on_with_explicit_opt_out(self) -> None:
        original_env = os.environ.pop("MATRIXARK_HOOK_AUTO_BATCH_EXTRACT", None)
        try:
            self.assertTrue(hook._env_bool("MATRIXARK_HOOK_AUTO_BATCH_EXTRACT", True))
            os.environ["MATRIXARK_HOOK_AUTO_BATCH_EXTRACT"] = "0"
            self.assertFalse(hook._env_bool("MATRIXARK_HOOK_AUTO_BATCH_EXTRACT", True))
            os.environ["MATRIXARK_HOOK_AUTO_BATCH_EXTRACT"] = "false"
            self.assertFalse(hook._env_bool("MATRIXARK_HOOK_AUTO_BATCH_EXTRACT", True))
            os.environ["MATRIXARK_HOOK_AUTO_BATCH_EXTRACT"] = "1"
            self.assertTrue(hook._env_bool("MATRIXARK_HOOK_AUTO_BATCH_EXTRACT", True))
        finally:
            if original_env is None:
                os.environ.pop("MATRIXARK_HOOK_AUTO_BATCH_EXTRACT", None)
            else:
                os.environ["MATRIXARK_HOOK_AUTO_BATCH_EXTRACT"] = original_env

        original_fast_env = os.environ.pop("MATRIXARK_HOOK_FAST_ASYNC_INGEST", None)
        try:
            self.assertTrue(hook._env_bool("MATRIXARK_HOOK_FAST_ASYNC_INGEST", True))
            os.environ["MATRIXARK_HOOK_FAST_ASYNC_INGEST"] = "off"
            self.assertFalse(hook._env_bool("MATRIXARK_HOOK_FAST_ASYNC_INGEST", True))
            os.environ["MATRIXARK_HOOK_FAST_ASYNC_INGEST"] = "yes"
            self.assertTrue(hook._env_bool("MATRIXARK_HOOK_FAST_ASYNC_INGEST", True))
        finally:
            if original_fast_env is None:
                os.environ.pop("MATRIXARK_HOOK_FAST_ASYNC_INGEST", None)
            else:
                os.environ["MATRIXARK_HOOK_FAST_ASYNC_INGEST"] = original_fast_env

        source = (Path(__file__).resolve().parents[1] / "tools" / "matrixark_codex_hook.py").read_text()
        self.assertIn('HOOK_AUTO_BATCH_EXTRACT = _env_bool("MATRIXARK_HOOK_AUTO_BATCH_EXTRACT", True)', source)
        self.assertIn('HOOK_FAST_ASYNC_INGEST = _env_bool("MATRIXARK_HOOK_FAST_ASYNC_INGEST", True)', source)

    def test_live_ingest_auto_batch_decision_covers_tool_but_not_boundaries(self) -> None:
        original_auto_batch = hook.HOOK_AUTO_BATCH_EXTRACT
        try:
            hook.HOOK_AUTO_BATCH_EXTRACT = True
            self.assertTrue(hook.should_auto_batch_extract_on_ingest("UserPromptSubmit"))
            self.assertTrue(hook.should_auto_batch_extract_on_ingest("PostToolUse"))
            self.assertFalse(hook.should_auto_batch_extract_on_ingest("Stop"))
            self.assertFalse(hook.should_auto_batch_extract_on_ingest("IdleTimeout"))

            hook.HOOK_AUTO_BATCH_EXTRACT = False
            self.assertFalse(hook.should_auto_batch_extract_on_ingest("UserPromptSubmit"))
            self.assertFalse(hook.should_auto_batch_extract_on_ingest("PostToolUse"))
        finally:
            hook.HOOK_AUTO_BATCH_EXTRACT = original_auto_batch

    def test_live_ingest_auto_batch_options_are_explicit_for_tool_and_stop(self) -> None:
        original_auto_batch = hook.HOOK_AUTO_BATCH_EXTRACT
        try:
            hook.HOOK_AUTO_BATCH_EXTRACT = True
            tool_args = hook.apply_hook_auto_batch_ingest_options(
                {},
                event="PostToolUse",
                session_commit_threshold=7,
                idle_commit_timeout_ms=123,
            )
            self.assertTrue(tool_args["auto_batch_extract"])
            self.assertEqual(7, tool_args["session_buffer_threshold"])
            self.assertEqual(123, tool_args["idle_commit_timeout_ms"])

            stop_args = hook.apply_hook_auto_batch_ingest_options(
                {"idle_commit_timeout_ms": 456},
                event="Stop",
                session_commit_threshold=7,
                idle_commit_timeout_ms=123,
            )
            self.assertFalse(stop_args["auto_batch_extract"])
            self.assertEqual(7, stop_args["session_buffer_threshold"])
            self.assertNotIn("idle_commit_timeout_ms", stop_args)

            hook.HOOK_AUTO_BATCH_EXTRACT = False
            disabled_args = hook.apply_hook_auto_batch_ingest_options(
                {},
                event="UserPromptSubmit",
                session_commit_threshold=7,
                idle_commit_timeout_ms=123,
            )
            self.assertFalse(disabled_args["auto_batch_extract"])
            self.assertEqual(7, disabled_args["session_buffer_threshold"])
            self.assertNotIn("idle_commit_timeout_ms", disabled_args)
        finally:
            hook.HOOK_AUTO_BATCH_EXTRACT = original_auto_batch

    def test_session_commit_extraction_options_match_fast_and_boundary_paths(self) -> None:
        args = Namespace(
            understanding_provider="rules",
            extraction_provider="rules",
            segment_provider="deterministic",
            segment_model="codex-memory-segmenter",
            segment_model_path="/models/codex-memory-segmenter",
            segment_max_new_tokens=96,
            segment_provider_fallback="deterministic",
            skip_prior_context=True,
        )

        options = hook.hook_session_commit_extraction_options(args)

        self.assertEqual("rules", options["understanding_provider"])
        self.assertEqual("rules", options["extraction_provider"])
        self.assertEqual("deterministic", options["segment_provider"])
        self.assertEqual("codex-memory-segmenter", options["segment_model"])
        self.assertEqual("/models/codex-memory-segmenter", options["segment_model_path"])
        self.assertEqual(96, options["segment_max_new_tokens"])
        self.assertEqual("deterministic", options["segment_provider_fallback"])
        self.assertTrue(options["skip_prior_context"])
        source = (Path(__file__).resolve().parents[1] / "tools" / "matrixark_codex_hook.py").read_text()
        self.assertIn("commit_extraction_options: Json = hook_session_commit_extraction_options(args)", source)
        self.assertIn("**hook_session_commit_extraction_options(args),", source)
        retrieve_call = source[
            source.index("\"matrixark_retrieve\"") : source.index(
                "append_hook_trace(server, trace, output=output, status=\"ok\")"
            )
        ]
        self.assertIn("**hook_session_commit_extraction_options(args),", retrieve_call)
        self.assertIn("\"cross_session\": codex_retrieve_cross_session_options(),", retrieve_call)

    def test_dual_hook_keeps_derived_context_out_of_raw_ingestion(self) -> None:
        script = (Path(__file__).resolve().parents[1] / "tools" / "matrixark_codex_dual_hook.sh").read_text()

        self.assertIn('f"{prefix}:raw_ingestion:records", raw_record', script)
        self.assertIn("for record in rust_live_extraction_records():", script)
        self.assertIn("for record in native_live_extraction_records():", script)
        self.assertIn('MATRIXARK_NATIVE_FULL_HOOK_PREFIX:-matrixark:mcp:codex', script)
        self.assertIn('MATRIXARK_RUST_FULL_HOOK_PREFIX:-matrixark:mcp:codex', script)

    def test_dual_hook_live_projection_emits_profile_embeddings_without_trivial_segments(self) -> None:
        script = (Path(__file__).resolve().parents[1] / "tools" / "matrixark_codex_dual_hook.sh").read_text()

        self.assertNotIn('"record_type": "context_segment"', script)
        self.assertIn('"memory_scope": "user_profile"', script)
        self.assertIn('"session_continuity": "cross_session"', script)
        self.assertIn('"record_type": "context_embedding"', script)
        self.assertIn('"embedding_type": "event_text"', script)
        self.assertIn('"embedding_type": "entity_state"', script)
        self.assertIn('"record_type": "context_summary_dirty"', script)
        self.assertIn('"memory_scope:user_profile"', script)
        self.assertIn('"session_continuity:cross_session"', script)

    def test_entity_dashboard_projects_state_as_value(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            adapter = MatrixArkLocalAdapter(Path(temporary_directory) / "events.jsonl")
            adapter.append(
                {
                    "record_type": "context_entity",
                    "entity_hash": 7,
                    "entity_type": "preference",
                    "entity_name": "build_preference",
                    "state": "Reuse the shared TemporalStore build.",
                    "updated_at_ms": 123,
                }
            )

            dashboard = adapter.ingestion_dashboard({"table": "entities"})

        self.assertEqual(1, dashboard["total"])
        self.assertEqual("Reuse the shared TemporalStore build.", dashboard["rows"][0]["value"])


    def test_codex_hook_messages_both_skips_local_proxy_debug_reader(self) -> None:
        original_native = matrixark_http._HookStoreReader
        original_service = matrixark_http._RustServiceHookStoreReader
        original_local = matrixark_http._RustLocalHookStoreReader
        calls = []

        class EmptyReader:
            name = "empty"

            def get_string(self, key):
                return "0"

            def hget(self, key, field):
                return None

        try:
            matrixark_http._HookStoreReader = lambda args: calls.append("native") or EmptyReader()
            matrixark_http._RustServiceHookStoreReader = lambda args: calls.append("rust-service") or EmptyReader()

            def fail_local(args):
                raise AssertionError("rust-local proxy should be explicit-only")

            matrixark_http._RustLocalHookStoreReader = fail_local
            matrixark_http.query_codex_hook_messages({"backend": "both", "top_k": 1})
        finally:
            matrixark_http._HookStoreReader = original_native
            matrixark_http._RustServiceHookStoreReader = original_service
            matrixark_http._RustLocalHookStoreReader = original_local

        self.assertEqual(["native", "rust-service"], calls)

    def test_query_effective_synthetic_status_uses_text_classifier(self) -> None:
        self.assertTrue(matrixark_http._hook_text_is_synthetic("matrixark plain string prompt hook proof 1784770203"))
        self.assertFalse(
            matrixark_http._hook_text_is_synthetic(
                "Fix MatrixArk query with validation/probe/synthetic rows hidden by default"
            )
        )

