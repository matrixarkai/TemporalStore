# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""_CodexPipelinePart3 methods split from test_matrixark_codex_hook_pipeline.MatrixArkCodexHookPipelineTest (mixin)."""
from __future__ import annotations

try:  # package path
    from tools.matrixark_mcp_core import *  # noqa: F401,F403
except ImportError:
    from matrixark_mcp_core import *  # noqa: F401,F403

try:  # names owned by the parent module
    from tools.test_matrixark_codex_hook_pipeline import (
    FastHookLocalAdapter,
    MatrixArkLocalAdapter,
    Namespace,
    NativeCaptureLocalAdapter,
    Path,
    infer_secondary_index_filter_groups,
    matrixark_codex_hook,
    matrixark_mcp_core,
    os,
    subprocess,
    sys,
    tempfile,
    time,
)
except ImportError:
    from test_matrixark_codex_hook_pipeline import (
    FastHookLocalAdapter,
    MatrixArkLocalAdapter,
    Namespace,
    NativeCaptureLocalAdapter,
    Path,
    infer_secondary_index_filter_groups,
    matrixark_codex_hook,
    matrixark_mcp_core,
    os,
    subprocess,
    sys,
    tempfile,
    time,
)


class _CodexPipelinePart3:
    def test_fast_hook_threshold_commit_recovers_pending_buffer_after_restart(self) -> None:
        original_auto_batch = matrixark_codex_hook.HOOK_AUTO_BATCH_EXTRACT
        matrixark_codex_hook.HOOK_AUTO_BATCH_EXTRACT = True
        try:
            with tempfile.TemporaryDirectory() as tmp_dir:
                event_log = Path(tmp_dir) / "matrixark-fast-hook-threshold-restart.jsonl"

                class Server:
                    def __init__(self, adapter: MatrixArkLocalAdapter) -> None:
                        self.adapter = adapter

                scope_args = {
                    "event": "UserPromptSubmit",
                    "account_id": "acct_fast_threshold_restart",
                    "tenant_id": "tenant_fast_threshold_restart",
                    "user_id": "user_fast_threshold_restart",
                    "session_id": "session_fast_threshold_restart",
                    "team": "codex",
                    "project": "temporalstore",
                    "session_commit_threshold": 2,
                    "idle_commit_timeout_ms": 300000,
                    "understanding_provider": "rules",
                    "segment_provider": "deterministic",
                }
                first_adapter = FastHookLocalAdapter(event_log)
                first = matrixark_codex_hook.fast_async_hook_ingest(
                    Server(first_adapter),
                    args=Namespace(**scope_args),
                    text="User prompt: restart recovery should keep the pending threshold buffer durable.",
                    role="user",
                    agent_context={"workspace_root": "/repo"},
                    hook={
                        "session_id_source": "payload_field",
                        "thread_id": "thread-fast-threshold-restart",
                        "turn_id": "turn-fast-threshold-restart-1",
                    },
                )
                self.assertEqual("accepted", first["status"])
                self.assertFalse(first["session_buffer"]["threshold_ready"])
                self.assertEqual("deferred", first["auto_batch_extract_result"]["status"])
                self.assertEqual("idle_timeout", first["auto_batch_extract_result"]["trigger_policy"])

                recovered_adapter = FastHookLocalAdapter(event_log)
                scope = {
                    "account_id": scope_args["account_id"],
                    "tenant_id": scope_args["tenant_id"],
                    "user_id": scope_args["user_id"],
                    "session_id": scope_args["session_id"],
                }
                recovered_pending = recovered_adapter.pending_session_events(scope)
                self.assertEqual(1, len(recovered_pending))
                self.assertIn("pending threshold buffer", recovered_pending[0]["text"])

                second = matrixark_codex_hook.fast_async_hook_ingest(
                    Server(recovered_adapter),
                    args=Namespace(**scope_args),
                    text="Assistant decision: restart recovery threshold commit extracted both buffered messages.",
                    role="assistant",
                    agent_context={"workspace_root": "/repo"},
                    hook={
                        "session_id_source": "payload_field",
                        "thread_id": "thread-fast-threshold-restart",
                        "turn_id": "turn-fast-threshold-restart-2",
                    },
                )
                commit = second["auto_batch_extract_result"]
                self.assertEqual("committed", commit["status"])
                self.assertEqual("threshold", commit["trigger_policy"])
                self.assertEqual(2, commit["committed_event_count"])
                self.assertEqual(["assistant", "user"], commit["source_roles"])
                self.assertEqual(2, commit["trigger_evidence"]["pending_event_count"])
                self.assertTrue(second["session_buffer"]["threshold_ready"])
                self.assertFalse(recovered_adapter.pending_session_events(scope))

                records = recovered_adapter.read_all()
                commits = [record for record in records if record.get("record_type") == "context_batch_commit"]
                self.assertEqual(1, len(commits))
                self.assertEqual(
                    [int(event_id) for event_id in commit["source_event_ids"]],
                    [int(event_id) for event_id in commits[0]["source_event_ids"]],
                )
                layers = commit["memory_layers_written"]
                self.assertGreaterEqual(layers["segments"], 1)
                self.assertGreaterEqual(layers["session_entities"], 1)
                self.assertGreaterEqual(layers["profile_entities"], 1)
                self.assertGreaterEqual(layers["secondary_indexes"], 1)
                self.assertGreaterEqual(layers["summary_dirty_nodes"], 1)
                self.assertGreaterEqual(
                    sum(1 for record in records if record.get("record_type") == "context_event"),
                    2,
                )
                self.assertTrue(any(record.get("record_type") == "context_index" for record in records))
                self.assertTrue(
                    any(
                        record.get("record_type") == "context_entity"
                        and record.get("memory_scope") == "user_profile"
                        and record.get("session_continuity") == "cross_session"
                        for record in records
                    )
                )

                reopened_again = FastHookLocalAdapter(event_log)
                pack = reopened_again.retrieve(
                    {
                        "scope": {**scope, "session_id": "session_fast_threshold_restart_followup"},
                        "session_scope": "prefer",
                        "query": "What assistant decision proved restart recovery threshold commit?",
                        "max_context_tokens": 180,
                        "audit_mode": "off",
                        "debug_context_pack": True,
                        "ranking": {"max_selected_refs": 4},
                    }
                )
                self.assertTrue(
                    any(
                        ref.get("ref_type") == "entity"
                        and ref.get("entity_type") == "assistant_decision"
                        and ref.get("memory_scope") == "user_profile"
                        and ref.get("session_continuity") == "cross_session"
                        and "restart recovery threshold commit" in str(ref.get("text") or "")
                        for ref in pack["selected_refs"]
                    ),
                    pack["selected_refs"],
                )
                budget = pack["recall_policy"]["memory_layer_budget"]
                self.assertGreaterEqual(budget["by_memory_scope"]["user_profile"]["refs"], 1)
                self.assertGreaterEqual(budget["by_session_continuity"]["cross_session"]["refs"], 1)
                self.assertIn("assistant", budget["source_message_counts_by_role"])
        finally:
            matrixark_codex_hook.HOOK_AUTO_BATCH_EXTRACT = original_auto_batch


    def test_idle_commit_worker_child_spawns_due_session_commit(self) -> None:
        launched: list[dict[str, object]] = []
        original_popen = matrixark_codex_hook.subprocess.Popen
        original_time = matrixark_codex_hook.time.time

        class FakePopen:
            def __init__(self, cmd, **kwargs) -> None:
                launched.append({"cmd": list(cmd), "kwargs": kwargs})

        try:
            matrixark_codex_hook.subprocess.Popen = FakePopen
            matrixark_codex_hook.time.time = lambda: 1000.0
            with tempfile.TemporaryDirectory() as tmp_dir:
                args = Namespace(
                    event="UserPromptSubmit",
                    backend="local",
                    event_log=Path(tmp_dir) / "matrixark-idle-worker.jsonl",
                    api_key="",
                    account_id="acct_idle_worker",
                    tenant_id="tenant_idle_worker",
                    user_id="user_idle_worker",
                    session_id="session_idle_worker",
                    team="codex",
                    project="temporalstore",
                    session_commit_threshold=20,
                    idle_commit_timeout_ms=250,
                    understanding_provider="rules",
                    segment_provider="deterministic",
                    request_timeout_ms=60000,
                    io_timeout_ms=60000,
                    repo_root=Path(tmp_dir),
                    metaserver="127.0.0.1:18000",
                    namespace="deploy_ns",
                    table="deploy_table",
                    temporalstore_lib="",
                    rust_proxy="",
                    rust_direct_sdk="",
                    rust_cli="",
                    storage_prefix="matrixark:codex-hook",
                    session_state_dir=Path(tmp_dir) / "sessions",
                    idle_commit_worker_only=False,
                    extraction_provider="rules",
                    segment_model="codex-memory-segmenter",
                    segment_model_path="/models/codex-memory-segmenter.gguf",
                    segment_max_new_tokens=96,
                    segment_provider_fallback="deterministic",
                    skip_prior_context=True,
                    idle_commit_cutoff_ms=0,
                )
                result = matrixark_codex_hook.spawn_idle_commit_worker_child(
                    args,
                    ingest={
                        "session_buffer": {
                            "idle_commit_scheduled": True,
                            "idle_commit_deadline_ms": 1000250,
                            "idle_commit_cutoff_ms": 1000000,
                        },
                        "auto_batch_extract_result": {
                            "trigger_policy": "idle_timeout",
                            "idle_commit_deadline_ms": 1000250,
                            "idle_commit_cutoff_ms": 1000000,
                        },
                    },
                    session_id_source="payload_field",
                )
            self.assertEqual("spawned", result["status"])
            self.assertEqual(250, result["delay_ms"])
            self.assertEqual(1, len(launched))
            cmd = launched[0]["cmd"]
            self.assertIn("--idle-commit-worker-only", cmd)
            self.assertIn("--event", cmd)
            self.assertIn("IdleTimeout", cmd)
            self.assertIn("--session-id", cmd)
            self.assertIn("session_idle_worker", cmd)
            self.assertIn("--idle-commit-cutoff-ms", cmd)
            self.assertIn("1000000", cmd)
            self.assertIn("--extraction-provider", cmd)
            self.assertIn("rules", cmd)
            self.assertIn("--segment-model", cmd)
            self.assertIn("codex-memory-segmenter", cmd)
            self.assertIn("--segment-model-path", cmd)
            self.assertIn("/models/codex-memory-segmenter.gguf", cmd)
            self.assertIn("--segment-max-new-tokens", cmd)
            self.assertIn("96", cmd)
            self.assertIn("--segment-provider-fallback", cmd)
            self.assertIn("deterministic", cmd)
            self.assertIn("--skip-prior-context", cmd)
            env = launched[0]["kwargs"]["env"]
            self.assertEqual("250", env["MATRIXARK_IDLE_COMMIT_WORKER_DELAY_MS"])
            self.assertEqual("1000000", env["MATRIXARK_IDLE_COMMIT_CUTOFF_MS"])
            self.assertEqual("UserPromptSubmit", env["MATRIXARK_IDLE_COMMIT_WORKER_PARENT_EVENT"])
        finally:
            matrixark_codex_hook.subprocess.Popen = original_popen
            matrixark_codex_hook.time.time = original_time


    def test_idle_commit_cutoff_leaves_newer_pending_events_uncommitted(self) -> None:
        original_auto_batch = matrixark_codex_hook.HOOK_AUTO_BATCH_EXTRACT
        original_hook_time = matrixark_codex_hook.time.time
        session_commit_globals = FastHookLocalAdapter.session_commit.__globals__
        original_adapter_now_ms = session_commit_globals["now_ms"]
        matrixark_codex_hook.HOOK_AUTO_BATCH_EXTRACT = True
        try:
            with tempfile.TemporaryDirectory() as tmp_dir:
                adapter = FastHookLocalAdapter(Path(tmp_dir) / "matrixark-idle-cutoff.jsonl")

                class Server:
                    def __init__(self) -> None:
                        self.adapter = adapter

                scope = {
                    "account_id": "acct_idle_cutoff",
                    "tenant_id": "tenant_idle_cutoff",
                    "user_id": "user_idle_cutoff",
                    "session_id": "session_idle_cutoff",
                }
                args = Namespace(
                    event="UserPromptSubmit",
                    **scope,
                    team="codex",
                    project="temporalstore",
                    session_commit_threshold=20,
                    idle_commit_timeout_ms=100,
                    understanding_provider="rules",
                    segment_provider="deterministic",
                )
                matrixark_codex_hook.time.time = lambda: 1000.0
                first = matrixark_codex_hook.fast_async_hook_ingest(
                    Server(),
                    args=args,
                    text="Older idle event should be committed alone after the cutoff.",
                    role="tool",
                    agent_context={"workspace_root": "/repo"},
                    hook={"session_id_source": "payload_field", "hook_type": "tool_result"},
                )
                self.assertTrue(first["session_buffer"]["idle_commit_scheduled"])
                cutoff_ms = first["session_buffer"]["idle_commit_cutoff_ms"]
                self.assertEqual(1000000, cutoff_ms)

                node_path = adapter.default_session_node_path(scope)
                node_hash = matrixark_mcp_core.stable_hash("/".join(node_path))
                newer_event_hash = matrixark_mcp_core.stable_hash("idle-cutoff-newer-event")
                newer_envelope = {
                    "kind": "message",
                    "scope": scope,
                    "messages": [{"role": "user", "content": "Newer prompt must remain pending for the next batch."}],
                    "metadata": {"source": "test", "hook_type": "before_llm", "codex_event": "UserPromptSubmit"},
                    "ingestion_time_ms": cutoff_ms + 50,
                }
                adapter.append(
                    {
                        "record_type": "context_event",
                        "event_id_hash": newer_event_hash,
                        "node_hash": node_hash,
                        "node_path": node_path,
                        "text": "Newer prompt must remain pending for the next batch.",
                        "summary_text": "Newer prompt must remain pending for the next batch.",
                        "classification": "PENDING_ASYNC_EXTRACTION",
                        "event_type": "pending_async",
                        "status": "pending",
                        "source_kind": "message",
                        "envelope": newer_envelope,
                        "updated_at_ms": cutoff_ms + 50,
                    }
                )
                adapter.append_session_buffer_event(
                    envelope=newer_envelope,
                    event_id_hash=newer_event_hash,
                    node_hash=node_hash,
                    node_path=node_path,
                    hook={"hook_type": "before_llm", "trigger": "UserPromptSubmit"},
                )

                session_commit_globals["now_ms"] = lambda: cutoff_ms + 150
                commit = adapter.session_commit(
                    {
                        "scope": scope,
                        "threshold_messages": 20,
                        "force": False,
                        "commit_reason": "idle_timeout",
                        "idle_timeout_ms": 100,
                        "commit_before_ms": cutoff_ms,
                    }
                )
                self.assertEqual("committed", commit["status"])
                self.assertEqual(1, commit["committed_event_count"])
                self.assertEqual(1, commit["pending_deferred_event_count"])
                self.assertEqual(cutoff_ms, commit["commit_before_ms"])
                self.assertNotIn(newer_event_hash, {int(event_id) for event_id in commit["source_event_ids"]})
                pending_after = adapter.pending_session_events(scope)
                self.assertEqual([newer_event_hash], [record.get("event_id_hash") for record in pending_after])
        finally:
            matrixark_codex_hook.HOOK_AUTO_BATCH_EXTRACT = original_auto_batch
            matrixark_codex_hook.time.time = original_hook_time
            session_commit_globals["now_ms"] = original_adapter_now_ms


    def test_retrieve_idle_preflush_respects_idle_commit_cutoff(self) -> None:
        original_auto_batch = matrixark_codex_hook.HOOK_AUTO_BATCH_EXTRACT
        original_hook_time = matrixark_codex_hook.time.time
        session_commit_globals = FastHookLocalAdapter.session_commit.__globals__
        original_adapter_now_ms = session_commit_globals["now_ms"]
        retrieve_request_globals = FastHookLocalAdapter.retrieve.__globals__["pre_retrieval_idle_commit_flush"].__globals__
        original_retrieve_now_ms = retrieve_request_globals["now_ms"]
        matrixark_codex_hook.HOOK_AUTO_BATCH_EXTRACT = True
        try:
            with tempfile.TemporaryDirectory() as tmp_dir:
                adapter = FastHookLocalAdapter(Path(tmp_dir) / "matrixark-retrieve-idle-cutoff.jsonl")

                class Server:
                    def __init__(self) -> None:
                        self.adapter = adapter

                scope = {
                    "account_id": "acct_retrieve_idle_cutoff",
                    "tenant_id": "tenant_retrieve_idle_cutoff",
                    "user_id": "user_retrieve_idle_cutoff",
                    "session_id": "session_retrieve_idle_cutoff",
                }
                args = Namespace(
                    event="UserPromptSubmit",
                    **scope,
                    team="codex",
                    project="temporalstore",
                    session_commit_threshold=20,
                    idle_commit_timeout_ms=100,
                    understanding_provider="rules",
                    segment_provider="deterministic",
                )
                matrixark_codex_hook.time.time = lambda: 2000.0
                first = matrixark_codex_hook.fast_async_hook_ingest(
                    Server(),
                    args=args,
                    text="Retrieval idle preflush should commit only the old assistant decision.",
                    role="assistant",
                    agent_context={"workspace_root": "/repo"},
                    hook={"session_id_source": "payload_field", "hook_type": "after_llm"},
                )
                cutoff_ms = first["session_buffer"]["idle_commit_cutoff_ms"]
                self.assertEqual(2000000, cutoff_ms)

                node_path = adapter.default_session_node_path(scope)
                node_hash = matrixark_mcp_core.stable_hash("/".join(node_path))
                newer_event_hash = matrixark_mcp_core.stable_hash("retrieve-idle-cutoff-newer-event")
                newer_envelope = {
                    "kind": "message",
                    "scope": scope,
                    "messages": [{"role": "user", "content": "Newer retrieval prompt should remain pending."}],
                    "metadata": {"source": "test", "hook_type": "before_llm", "codex_event": "UserPromptSubmit"},
                    "ingestion_time_ms": cutoff_ms + 25,
                }
                adapter.append(
                    {
                        "record_type": "context_event",
                        "event_id_hash": newer_event_hash,
                        "node_hash": node_hash,
                        "node_path": node_path,
                        "text": "Newer retrieval prompt should remain pending.",
                        "summary_text": "Newer retrieval prompt should remain pending.",
                        "classification": "PENDING_ASYNC_EXTRACTION",
                        "event_type": "pending_async",
                        "status": "pending",
                        "source_kind": "message",
                        "envelope": newer_envelope,
                        "updated_at_ms": cutoff_ms + 25,
                    }
                )
                adapter.append_session_buffer_event(
                    envelope=newer_envelope,
                    event_id_hash=newer_event_hash,
                    node_hash=node_hash,
                    node_path=node_path,
                    hook={"hook_type": "before_llm", "trigger": "UserPromptSubmit"},
                )

                session_commit_globals["now_ms"] = lambda: cutoff_ms + 150
                retrieve_request_globals["now_ms"] = lambda: cutoff_ms + 150
                pack = adapter.retrieve(
                    {
                        "scope": scope,
                        "session_scope": "prefer",
                        "query": "What old assistant decision should retrieval preflush expose?",
                        "max_context_tokens": 240,
                        "audit_mode": "off",
                        "debug_context_pack": True,
                        "ranking": {"max_selected_refs": 4, "min_similarity_score": 0.0},
                    }
                )
                preflush = pack["retrieval_metrics"]["pre_retrieval_idle_commit"]
                self.assertEqual("committed", preflush["status"])
                self.assertEqual(1, preflush["committed_event_count"])
                self.assertEqual(1, preflush["pending_deferred_event_count"])
                self.assertEqual(cutoff_ms, preflush["commit_before_ms"])
                self.assertNotIn(newer_event_hash, {int(event_id) for event_id in preflush["source_event_ids"]})
                pending_after = adapter.pending_session_events(scope)
                self.assertEqual([newer_event_hash], [record.get("event_id_hash") for record in pending_after])
        finally:
            matrixark_codex_hook.HOOK_AUTO_BATCH_EXTRACT = original_auto_batch
            matrixark_codex_hook.time.time = original_hook_time
            session_commit_globals["now_ms"] = original_adapter_now_ms
            retrieve_request_globals["now_ms"] = original_retrieve_now_ms

    def test_retrieve_idle_preflush_flushes_all_due_cutoffs(self) -> None:
        original_auto_batch = matrixark_codex_hook.HOOK_AUTO_BATCH_EXTRACT
        original_hook_time = matrixark_codex_hook.time.time
        session_commit_globals = FastHookLocalAdapter.session_commit.__globals__
        original_adapter_now_ms = session_commit_globals["now_ms"]
        retrieve_request_globals = FastHookLocalAdapter.retrieve.__globals__["pre_retrieval_idle_commit_flush"].__globals__
        original_retrieve_now_ms = retrieve_request_globals["now_ms"]
        matrixark_codex_hook.HOOK_AUTO_BATCH_EXTRACT = True
        try:
            with tempfile.TemporaryDirectory() as tmp_dir:
                adapter = FastHookLocalAdapter(Path(tmp_dir) / "matrixark-retrieve-idle-all-due.jsonl")

                class Server:
                    def __init__(self) -> None:
                        self.adapter = adapter

                scope = {
                    "account_id": "acct_retrieve_idle_all_due",
                    "tenant_id": "tenant_retrieve_idle_all_due",
                    "user_id": "user_retrieve_idle_all_due",
                    "session_id": "session_retrieve_idle_all_due",
                }
                args = Namespace(
                    event="UserPromptSubmit",
                    **scope,
                    team="codex",
                    project="temporalstore",
                    session_commit_threshold=20,
                    idle_commit_timeout_ms=100,
                    understanding_provider="rules",
                    segment_provider="deterministic",
                )

                matrixark_codex_hook.time.time = lambda: 3000.0
                first = matrixark_codex_hook.fast_async_hook_ingest(
                    Server(),
                    args=args,
                    text="First due idle prompt says profile memory should include the rust repo path.",
                    role="user",
                    agent_context={"workspace_root": "/repo"},
                    hook={"session_id_source": "payload_field", "hook_type": "before_llm"},
                )
                first_cutoff_ms = first["session_buffer"]["idle_commit_cutoff_ms"]

                matrixark_codex_hook.time.time = lambda: 3000.050
                second = matrixark_codex_hook.fast_async_hook_ingest(
                    Server(),
                    args=args,
                    text="Second due idle prompt says retrieval should flush through the latest due cutoff.",
                    role="assistant",
                    agent_context={"workspace_root": "/repo"},
                    hook={"session_id_source": "payload_field", "hook_type": "after_llm"},
                )
                second_cutoff_ms = second["session_buffer"]["idle_commit_cutoff_ms"]
                self.assertGreater(second_cutoff_ms, first_cutoff_ms)

                session_commit_globals["now_ms"] = lambda: second_cutoff_ms + 150
                retrieve_request_globals["now_ms"] = lambda: second_cutoff_ms + 150
                pack = adapter.retrieve(
                    {
                        "scope": scope,
                        "session_scope": "prefer",
                        "query": "What due idle prompts should retrieval preflush expose?",
                        "max_context_tokens": 360,
                        "audit_mode": "off",
                        "debug_context_pack": True,
                        "ranking": {"max_selected_refs": 6, "min_similarity_score": 0.0},
                    }
                )

                preflush = pack["retrieval_metrics"]["pre_retrieval_idle_commit"]
                self.assertEqual("committed", preflush["status"])
                self.assertEqual(2, preflush["due_task_count"])
                self.assertEqual(2, preflush["resolved_scheduled_task_count"])
                self.assertEqual(2, preflush["committed_event_count"])
                self.assertEqual(second_cutoff_ms, preflush["commit_before_ms"])
                self.assertEqual([], adapter.pending_session_events(scope))
        finally:
            matrixark_codex_hook.HOOK_AUTO_BATCH_EXTRACT = original_auto_batch
            matrixark_codex_hook.time.time = original_hook_time
            session_commit_globals["now_ms"] = original_adapter_now_ms
            retrieve_request_globals["now_ms"] = original_retrieve_now_ms

    def test_fast_hook_idle_preflush_persists_real_adapter_memory_layers(self) -> None:
        original_auto_batch = matrixark_codex_hook.HOOK_AUTO_BATCH_EXTRACT
        matrixark_codex_hook.HOOK_AUTO_BATCH_EXTRACT = True
        try:
            with tempfile.TemporaryDirectory() as tmp_dir:
                adapter = FastHookLocalAdapter(Path(tmp_dir) / "matrixark-fast-hook-idle.jsonl")

                class Server:
                    def __init__(self) -> None:
                        self.adapter = adapter

                scope_args = {
                    "event": "UserPromptSubmit",
                    "account_id": "acct_fast_idle",
                    "tenant_id": "tenant_fast_idle",
                    "user_id": "user_fast_idle",
                    "session_id": "session_fast_idle",
                    "team": "codex",
                    "project": "temporalstore",
                    "session_commit_threshold": 20,
                    "idle_commit_timeout_ms": 1,
                    "understanding_provider": "rules",
                    "segment_provider": "deterministic",
                }
                server = Server()
                first = matrixark_codex_hook.fast_async_hook_ingest(
                    server,
                    args=Namespace(**scope_args),
                    text="Tool evidence before idle: Exit code: 0 and hook pipeline tests passed.",
                    role="tool",
                    agent_context={"workspace_root": "/repo"},
                    hook={
                        "session_id_source": "payload_field",
                        "thread_id": "thread-fast-idle",
                        "turn_id": "turn-fast-idle-1",
                    },
                )
                self.assertEqual("accepted", first["status"])
                self.assertFalse(first["session_buffer"]["threshold_ready"])
                self.assertFalse(first["idle_commit_result"])
                self.assertTrue(first["session_buffer"]["idle_commit_scheduled"])
                self.assertEqual("deferred", first["auto_batch_extract_result"]["status"])
                self.assertEqual("idle_timeout", first["auto_batch_extract_result"]["trigger_policy"])
                self.assertEqual("session_buffer_idle_deadline_scheduled", first["auto_batch_extract_result"]["reason"])
                self.assertEqual(1, first["auto_batch_extract_result"]["pending_event_count"])
                self.assertEqual(1, first["auto_batch_extract_result"]["pending_message_count"])
                time.sleep(0.05)

                second = matrixark_codex_hook.fast_async_hook_ingest(
                    server,
                    args=Namespace(**scope_args),
                    text="New user prompt should not mix into the previous idle batch.",
                    role="user",
                    agent_context={"workspace_root": "/repo"},
                    hook={
                        "session_id_source": "payload_field",
                        "thread_id": "thread-fast-idle",
                        "turn_id": "turn-fast-idle-2",
                    },
                )
                idle_commit = second["idle_commit_result"]
                self.assertEqual("committed", idle_commit["status"])
                self.assertEqual("idle_timeout", idle_commit["trigger_policy"])
                self.assertEqual("provisional", idle_commit["extraction_phase"])
                self.assertFalse(idle_commit["final_session_boundary"])
                self.assertEqual(1, idle_commit["committed_event_count"])
                self.assertEqual(["tool"], idle_commit["source_roles"])
                self.assertTrue(second["session_buffer"]["pre_ingest_idle_ready"])
                self.assertGreaterEqual(second["session_buffer"]["pre_ingest_idle_elapsed_ms"], 1)
                self.assertEqual(idle_commit, second["auto_batch_extract_result"])

                layers = idle_commit["memory_layers_written"]
                self.assertGreaterEqual(layers["session_entities"], 1)
                self.assertGreaterEqual(layers["profile_entities"], 1)
                self.assertGreaterEqual(layers["secondary_indexes"], 1)
                self.assertGreaterEqual(layers["summary_dirty_nodes"], 1)

                scope = {
                    "account_id": "acct_fast_idle",
                    "tenant_id": "tenant_fast_idle",
                    "user_id": "user_fast_idle",
                    "session_id": "session_fast_idle",
                }
                pending_after_preflush = adapter.pending_session_events(scope)
                self.assertEqual(1, len(pending_after_preflush))
                self.assertIn("New user prompt", pending_after_preflush[0]["text"])

                records = adapter.read_all()
                record_types = {record.get("record_type") for record in records}
                for record_type in {
                    "context_event",
                    "context_embedding",
                    "context_segment",
                    "context_entity",
                    "context_index",
                    "context_summary_dirty",
                    "context_extraction_audit",
                    "context_batch_commit",
                }:
                    self.assertIn(record_type, record_types)
                commits = [record for record in records if record.get("record_type") == "context_batch_commit"]
                self.assertEqual(1, len(commits))
                self.assertEqual("idle_timeout", commits[0]["trigger_policy"])
                self.assertEqual(["tool"], commits[0]["source_roles"])
                self.assertGreaterEqual(commits[0]["memory_layers_written"]["session_entities"], 1)
                self.assertGreaterEqual(commits[0]["memory_layers_written"]["profile_entities"], 1)
                self.assertGreaterEqual(commits[0]["memory_layers_written"]["secondary_indexes"], 1)
                self.assertTrue(commits[0]["trigger_evidence"]["idle_ready"])
                self.assertEqual(1, commits[0]["trigger_evidence"]["pending_event_count"])
                self.assertGreaterEqual(
                    sum(1 for record in records if record.get("record_type") == "context_event"),
                    2,
                )
                committed_event_hashes = {int(event_id) for event_id in idle_commit["source_event_ids"]}
                committed_events = [
                    record
                    for record in records
                    if record.get("record_type") == "context_event"
                    and record.get("status") == "extraction_committed"
                    and record.get("event_id_hash") in committed_event_hashes
                ]
                self.assertEqual(1, len(committed_events))
                committed_event_embeddings = [
                    record
                    for record in records
                    if record.get("record_type") == "context_embedding"
                    and record.get("embedding_type") == "event_text"
                    and record.get("ref_hash") in committed_event_hashes
                ]
                self.assertGreaterEqual(len(committed_event_embeddings), 1)
                self.assertEqual("session", committed_event_embeddings[0]["memory_scope"])
                self.assertEqual("same_session", committed_event_embeddings[0]["session_continuity"])
                self.assertNotIn("extraction_phase", committed_event_embeddings[0])
                self.assertNotIn("final_session_boundary", committed_event_embeddings[0])
                self.assertNotIn("source_roles", committed_event_embeddings[0])
                self.assertNotIn("source_role_counts", committed_event_embeddings[0])
                self.assertNotIn("source_hook_types", committed_event_embeddings[0])
                self.assertNotIn("source_hook_type_counts", committed_event_embeddings[0])
                self.assertNotIn("source_codex_events", committed_event_embeddings[0])
                self.assertNotIn("source_codex_event_counts", committed_event_embeddings[0])
                self.assertNotIn("extraction_context_event_ids", committed_event_embeddings[0])
                extraction_audits = [
                    record
                    for record in records
                    if record.get("record_type") == "context_extraction_audit"
                    and record.get("batch_id_hash") == idle_commit["batch_id_hash"]
                ]
                self.assertEqual(1, len(extraction_audits))
                audit_outputs = extraction_audits[0]["outputs"]
                self.assertEqual("always_when_profile_scope_available", audit_outputs["profile_promotion_policy"])
                self.assertTrue(audit_outputs["profile_promotion_scope_available"])
                self.assertEqual(audit_outputs["entities"], audit_outputs["profile_entities"])
                self.assertGreaterEqual(audit_outputs["indexes"], audit_outputs["entity_indexes"])
                self.assertTrue(
                    any(
                        record.get("record_type") == "context_entity"
                        and record.get("memory_scope") == "session"
                        for record in records
                    )
                )
                self.assertTrue(
                    any(
                        record.get("record_type") == "context_entity"
                        and record.get("memory_scope") == "user_profile"
                        and record.get("session_continuity") == "cross_session"
                        for record in records
                    )
                )
                progress = [
                    record
                    for record in records
                    if record.get("record_type") == "matrixark_async_pipeline_task"
                    and record.get("status") == "extraction_committed"
                    and record.get("commit_id_hash") == idle_commit["commit_id_hash"]
                ]
                self.assertEqual(
                    [int(event_id) for event_id in idle_commit["source_event_ids"]],
                    [int(record["event_id_hash"]) for record in progress],
                )
                self.assertTrue(all(record["completed_stages"] == ["extraction"] for record in progress))
                self.assertTrue(all("summary" in record["remaining_stages"] for record in progress))

                pack = adapter.retrieve(
                    {
                        "scope": {
                            "account_id": "acct_fast_idle",
                            "tenant_id": "tenant_fast_idle",
                            "user_id": "user_fast_idle",
                            "session_id": "session_fast_idle_followup",
                        },
                        "session_scope": "prefer",
                        "query": "What tool evidence was captured by the idle timeout memory commit?",
                        "max_context_tokens": 120,
                        "audit_mode": "off",
                        "debug_context_pack": True,
                        "ranking": {"max_selected_refs": 3},
                    }
                )
                self.assertLessEqual(pack["used_context_tokens"], 120)
                self.assertTrue(
                    any(
                        ref.get("ref_type") == "entity"
                        and ref.get("entity_type") == "tool_evidence"
                        and ref.get("memory_scope") == "user_profile"
                        and ref.get("session_continuity") == "cross_session"
                        and "hook pipeline tests passed" in str(ref.get("text") or ref.get("summary_text") or "")
                        for ref in pack["selected_refs"]
                    ),
                    pack["selected_refs"],
                )
                memory_budget = pack["recall_policy"]["memory_layer_budget"]
                self.assertIn("user_profile", memory_budget["by_memory_scope"])
                self.assertIn("cross_session", memory_budget["by_session_continuity"])
                self.assertGreaterEqual(memory_budget["source_message_counts_by_role"].get("tool", 0), 1)
                self.assertGreaterEqual(memory_budget["source_message_counts_by_role"].get("user", 0), 1)
                session_only_pack = adapter.retrieve(
                    {
                        "scope": {
                            "account_id": "acct_fast_idle",
                            "tenant_id": "tenant_fast_idle",
                            "user_id": "user_fast_idle",
                            "session_id": "session_fast_idle_followup",
                        },
                        "session_scope": "only",
                        "query": "What tool evidence was captured by the idle timeout memory commit?",
                        "max_context_tokens": 120,
                        "audit_mode": "off",
                        "ranking": {"max_selected_refs": 3},
                    }
                )
                session_only_refs = session_only_pack.get("selected_refs", [])
                self.assertFalse(
                    any(ref.get("session_continuity") == "cross_session" for ref in session_only_refs),
                    session_only_refs,
                )
                self.assertFalse(
                    any(ref.get("memory_scope") == "user_profile" for ref in session_only_refs),
                    session_only_refs,
                )
                session_only_budget = session_only_pack.get("recall_policy", {}).get("memory_layer_budget", {})
                self.assertEqual(0, session_only_budget.get("by_memory_scope", {}).get("user_profile", {}).get("refs", 0))
                self.assertEqual(0, session_only_budget.get("by_session_continuity", {}).get("cross_session", {}).get("refs", 0))
                session_only_cross_policy = session_only_pack.get("recall_policy", {}).get("cross_session", {})
                if session_only_cross_policy:
                    self.assertFalse(session_only_cross_policy["enabled"])
                    self.assertEqual(0, session_only_cross_policy["budget_tokens"])
        finally:
            matrixark_codex_hook.HOOK_AUTO_BATCH_EXTRACT = original_auto_batch

    def test_retrieval_idle_preflush_reports_committed_memory_layers_and_selection_budget(self) -> None:
        original_auto_batch = matrixark_codex_hook.HOOK_AUTO_BATCH_EXTRACT
        matrixark_codex_hook.HOOK_AUTO_BATCH_EXTRACT = True
        try:
            with tempfile.TemporaryDirectory() as tmp_dir:
                adapter = FastHookLocalAdapter(Path(tmp_dir) / "matrixark-retrieve-idle-preflush.jsonl")

                class Server:
                    def __init__(self) -> None:
                        self.adapter = adapter

                scope_args = {
                    "event": "UserPromptSubmit",
                    "account_id": "acct_retrieve_idle",
                    "tenant_id": "tenant_retrieve_idle",
                    "user_id": "user_retrieve_idle",
                    "session_id": "session_retrieve_idle",
                    "team": "codex",
                    "project": "temporalstore",
                    "session_commit_threshold": 20,
                    "idle_commit_timeout_ms": 1,
                    "understanding_provider": "rules",
                    "segment_provider": "deterministic",
                }
                matrixark_codex_hook.fast_async_hook_ingest(
                    Server(),
                    args=Namespace(**scope_args),
                    text="Decision: retrieval idle preflush must expose profile memory layers and assistant selection budgets.",
                    role="assistant",
                    agent_context={"workspace_root": "/repo"},
                    hook={
                        "session_id_source": "payload_field",
                        "thread_id": "thread-retrieve-idle",
                        "turn_id": "turn-retrieve-idle-1",
                    },
                )
                time.sleep(0.05)
                scope = {
                    "account_id": "acct_retrieve_idle",
                    "tenant_id": "tenant_retrieve_idle",
                    "user_id": "user_retrieve_idle",
                    "session_id": "session_retrieve_idle",
                }
                pack = adapter.retrieve(
                    {
                        "scope": scope,
                        "session_scope": "prefer",
                        "query": "What did retrieval idle preflush decide about profile memory layers?",
                        "max_context_tokens": 220,
                        "audit_mode": "off",
                        "debug_context_pack": True,
                        "ranking": {
                            "max_selected_refs": 4,
                            "min_similarity_score": 0.0,
                            "budget_fill_policy": "force_fill",
                        },
                    }
                )
                preflush = pack["retrieval_metrics"]["pre_retrieval_idle_commit"]
                self.assertEqual("committed", preflush["status"])
                self.assertEqual("idle_timeout", preflush["trigger_policy"])
                self.assertEqual(1, preflush["committed_event_count"])
                self.assertEqual({"assistant": 1}, preflush["source_role_counts"])
                self.assertEqual(
                    {"selected_assistant_decision_outcome_only": 1},
                    preflush["source_memory_selection_policy_counts"],
                )
                self.assertGreaterEqual(preflush["memory_layers_written"]["session_entities"], 1)
                self.assertGreaterEqual(preflush["memory_layers_written"]["profile_entities"], 1)
                self.assertGreaterEqual(preflush["memory_layers_written"]["secondary_indexes"], 1)
                self.assertTrue(preflush["summary_refresh"]["profile_summary_refresh_required"])
                refresh_metrics = pack["retrieval_metrics"]["pre_retrieval_summary_refresh"]
                self.assertTrue(refresh_metrics["enabled"])
                self.assertEqual("fresh_idle_commit", refresh_metrics["source"])
                self.assertEqual("refreshed", refresh_metrics["status"])
                self.assertGreaterEqual(refresh_metrics["refreshed_count"], 1)
                self.assertTrue(refresh_metrics["fresh_idle_commit_dirty"])
                self.assertTrue(refresh_metrics["fresh_idle_commit_summary_required"])
                self.assertEqual(1, refresh_metrics["fresh_idle_commit_committed_event_count"])
                self.assertTrue(refresh_metrics["fresh_idle_commit_profile_summary_required"])
                self.assertGreaterEqual(refresh_metrics["fresh_idle_commit_summary_dirty_nodes"], 1)
                self.assertTrue(
                    any(
                        ref.get("entity_type") == "assistant_decision"
                        and ref.get("memory_scope") == "user_profile"
                        for ref in pack["selected_refs"]
                    ),
                    pack["selected_refs"],
                )
                self.assertTrue(
                    any(
                        ref.get("ref_type") == "summary"
                        and ref.get("memory_scope") == "user_profile"
                        for ref in pack["selected_refs"]
                    ),
                    pack["selected_refs"],
                )
        finally:
            matrixark_codex_hook.HOOK_AUTO_BATCH_EXTRACT = original_auto_batch

    def test_fast_hook_idle_preflush_recovers_pending_buffer_after_restart(self) -> None:
        original_auto_batch = matrixark_codex_hook.HOOK_AUTO_BATCH_EXTRACT
        matrixark_codex_hook.HOOK_AUTO_BATCH_EXTRACT = True
        try:
            with tempfile.TemporaryDirectory() as tmp_dir:
                event_log = Path(tmp_dir) / "matrixark-fast-hook-idle-restart.jsonl"

                class Server:
                    def __init__(self, adapter: MatrixArkLocalAdapter) -> None:
                        self.adapter = adapter

                scope_args = {
                    "event": "UserPromptSubmit",
                    "account_id": "acct_fast_idle_restart",
                    "tenant_id": "tenant_fast_idle_restart",
                    "user_id": "user_fast_idle_restart",
                    "session_id": "session_fast_idle_restart",
                    "team": "codex",
                    "project": "temporalstore",
                    "session_commit_threshold": 20,
                    "idle_commit_timeout_ms": 1,
                    "understanding_provider": "rules",
                    "segment_provider": "deterministic",
                }
                first_adapter = FastHookLocalAdapter(event_log)
                first = matrixark_codex_hook.fast_async_hook_ingest(
                    Server(first_adapter),
                    args=Namespace(**scope_args),
                    text="Tool evidence before restart idle: Exit code: 0 and restart idle recovery passed.",
                    role="tool",
                    agent_context={"workspace_root": "/repo"},
                    hook={
                        "session_id_source": "payload_field",
                        "thread_id": "thread-fast-idle-restart",
                        "turn_id": "turn-fast-idle-restart-1",
                    },
                )
                self.assertEqual("accepted", first["status"])
                self.assertFalse(first["session_buffer"]["threshold_ready"])
                self.assertFalse(first["idle_commit_result"])
                first_auto_batch = first["auto_batch_extract_result"]
                self.assertEqual("deferred", first_auto_batch["status"])
                self.assertEqual("idle_timeout", first_auto_batch["trigger_policy"])
                self.assertTrue(first_auto_batch["idle_commit_scheduled"])
                time.sleep(0.05)

                recovered_adapter = FastHookLocalAdapter(event_log)
                scope = {
                    "account_id": scope_args["account_id"],
                    "tenant_id": scope_args["tenant_id"],
                    "user_id": scope_args["user_id"],
                    "session_id": scope_args["session_id"],
                }
                recovered_pending = recovered_adapter.pending_session_events(scope)
                self.assertEqual(1, len(recovered_pending))
                self.assertIn("restart idle recovery", recovered_pending[0]["text"])

                second = matrixark_codex_hook.fast_async_hook_ingest(
                    Server(recovered_adapter),
                    args=Namespace(**scope_args),
                    text="New prompt after restart should preflush the older idle batch before entering the buffer.",
                    role="user",
                    agent_context={"workspace_root": "/repo"},
                    hook={
                        "session_id_source": "payload_field",
                        "thread_id": "thread-fast-idle-restart",
                        "turn_id": "turn-fast-idle-restart-2",
                    },
                )
                idle_commit = second["idle_commit_result"]
                self.assertEqual("committed", idle_commit["status"])
                self.assertEqual("idle_timeout", idle_commit["trigger_policy"])
                self.assertEqual("provisional", idle_commit["extraction_phase"])
                self.assertFalse(idle_commit["final_session_boundary"])
                self.assertEqual(1, idle_commit["committed_event_count"])
                self.assertEqual(["tool"], idle_commit["source_roles"])
                self.assertTrue(second["session_buffer"]["pre_ingest_idle_ready"])
                self.assertGreaterEqual(second["session_buffer"]["pre_ingest_idle_elapsed_ms"], 1)
                self.assertEqual(idle_commit, second["auto_batch_extract_result"])

                pending_after_preflush = recovered_adapter.pending_session_events(scope)
                self.assertEqual(1, len(pending_after_preflush))
                self.assertIn("New prompt after restart", pending_after_preflush[0]["text"])

                records = recovered_adapter.read_all()
                commits = [record for record in records if record.get("record_type") == "context_batch_commit"]
                self.assertEqual(1, len(commits))
                self.assertEqual("idle_timeout", commits[0]["trigger_policy"])
                self.assertEqual(
                    [int(event_id) for event_id in idle_commit["source_event_ids"]],
                    [int(event_id) for event_id in commits[0]["source_event_ids"]],
                )
                layers = idle_commit["memory_layers_written"]
                self.assertGreaterEqual(layers["session_entities"], 1)
                self.assertGreaterEqual(layers["profile_entities"], 1)
                self.assertGreaterEqual(layers["secondary_indexes"], 1)
                self.assertGreaterEqual(layers["summary_dirty_nodes"], 1)
                self.assertGreaterEqual(
                    sum(1 for record in records if record.get("record_type") == "context_event"),
                    2,
                )
                self.assertTrue(any(record.get("record_type") == "context_index" for record in records))
                self.assertTrue(
                    any(
                        record.get("record_type") == "context_entity"
                        and record.get("memory_scope") == "user_profile"
                        and record.get("session_continuity") == "cross_session"
                        for record in records
                    )
                )
                progress = [
                    record
                    for record in records
                    if record.get("record_type") == "matrixark_async_pipeline_task"
                    and record.get("status") == "extraction_committed"
                    and record.get("commit_id_hash") == idle_commit["commit_id_hash"]
                ]
                self.assertEqual(
                    [int(event_id) for event_id in idle_commit["source_event_ids"]],
                    [int(record["event_id_hash"]) for record in progress],
                )
                self.assertTrue(all(record["completed_stages"] == ["extraction"] for record in progress))
                self.assertTrue(all("summary" in record["remaining_stages"] for record in progress))

                reopened_again = FastHookLocalAdapter(event_log)
                pack = reopened_again.retrieve(
                    {
                        "scope": {**scope, "session_id": "session_fast_idle_restart_followup"},
                        "session_scope": "prefer",
                        "query": "What tool evidence proved restart idle recovery?",
                        "max_context_tokens": 180,
                        "audit_mode": "off",
                        "debug_context_pack": True,
                        "ranking": {"max_selected_refs": 4},
                    }
                )
                self.assertLessEqual(pack["used_context_tokens"], 180)
                self.assertTrue(
                    any(
                        ref.get("ref_type") == "entity"
                        and ref.get("entity_type") == "tool_evidence"
                        and ref.get("memory_scope") == "user_profile"
                        and ref.get("session_continuity") == "cross_session"
                        and "restart idle recovery passed" in str(ref.get("text") or ref.get("summary_text") or "")
                        for ref in pack["selected_refs"]
                    ),
                    pack["selected_refs"],
                )
                budget = pack["recall_policy"]["memory_layer_budget"]
                self.assertGreaterEqual(budget["by_memory_scope"]["user_profile"]["refs"], 1)
                self.assertGreaterEqual(budget["by_session_continuity"]["cross_session"]["refs"], 1)
                self.assertEqual(1, budget["source_message_counts_by_role"].get("tool"))
                self.assertGreaterEqual(budget["source_message_counts_by_role"].get("user", 0), 1)
        finally:
            matrixark_codex_hook.HOOK_AUTO_BATCH_EXTRACT = original_auto_batch

    def test_local_native_context_pack_receives_source_role_budget_tokens(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            adapter = NativeCaptureLocalAdapter(Path(tmp_dir) / "matrixark-native-budget.jsonl")
            scope = {
                "account_id": "acct_native_budget",
                "tenant_id": "tenant_native_budget",
                "user_id": "user_native_budget",
                "session_id": "session_native_budget",
            }
            pack = adapter.retrieve(
                {
                    "scope": scope,
                    "query": "GPU budget",
                    "max_context_tokens": 256,
                    "source_role_budget_tokens": {"assistant": 64, "tool": 32},
                    "ranking": {"max_selected_refs": 4},
                    "audit_mode": "off",
                    "debug_context_pack": True,
                }
            )

            self.assertEqual("local-native-pack", pack["pack_id"])
            self.assertEqual(1, len(adapter.native_requests))
            request = adapter.native_requests[0]
            self.assertEqual({"assistant": 64, "tool": 32}, request["source_role_budget_tokens"])
            self.assertEqual({"max_selected_refs": 4}, request["ranking"])

    def test_local_native_context_pack_receives_memory_selection_policy_budget_tokens(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            adapter = NativeCaptureLocalAdapter(Path(tmp_dir) / "matrixark-native-selection-policy-budget.jsonl")
            scope = {
                "account_id": "acct_native_selection_policy_budget",
                "tenant_id": "tenant_native_selection_policy_budget",
                "user_id": "user_native_selection_policy_budget",
                "session_id": "session_native_selection_policy_budget",
            }
            pack = adapter.retrieve(
                {
                    "scope": scope,
                    "query": "selected tool evidence budget",
                    "max_context_tokens": 256,
                    "memory_selection_policy_budget_tokens": {
                        "selected_assistant_decision_outcome_only": 24,
                        "selected_tool_evidence_only": 48,
                    },
                    "ranking": {"max_selected_refs": 4},
                    "audit_mode": "off",
                    "debug_context_pack": True,
                }
            )

            self.assertEqual("local-native-pack", pack["pack_id"])
            self.assertEqual(1, len(adapter.native_requests))
            request = adapter.native_requests[0]
            self.assertEqual(
                {
                    "selected_assistant_decision_outcome_only": 24,
                    "selected_tool_evidence_only": 48,
                },
                request["memory_selection_policy_budget_tokens"],
            )
            self.assertEqual("explicit", request["memory_selection_policy_budget_mode"])

    def test_local_native_context_pack_receives_extraction_phase_budget_tokens(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            adapter = NativeCaptureLocalAdapter(Path(tmp_dir) / "matrixark-native-extraction-phase-budget.jsonl")
            scope = {
                "account_id": "acct_native_phase_budget",
                "tenant_id": "tenant_native_phase_budget",
                "user_id": "user_native_phase_budget",
                "session_id": "session_native_phase_budget",
            }
            pack = adapter.retrieve(
                {
                    "scope": scope,
                    "query": "final and provisional memory budget",
                    "max_context_tokens": 256,
                    "extraction_phase_budget_tokens": {
                        "pending_async": 16,
                        "provisional": 32,
                        "final": 96,
                    },
                    "ranking": {"max_selected_refs": 4},
                    "audit_mode": "off",
                    "debug_context_pack": True,
                }
            )

            self.assertEqual("local-native-pack", pack["pack_id"])
            self.assertEqual(1, len(adapter.native_requests))
            request = adapter.native_requests[0]
            self.assertEqual(
                {"pending_async": 16, "provisional": 32, "final": 96},
                request["extraction_phase_budget_tokens"],
            )
            self.assertEqual("explicit", request["extraction_phase_budget_mode"])

    def test_local_native_context_pack_receives_auto_source_role_budget_tokens(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            adapter = NativeCaptureLocalAdapter(Path(tmp_dir) / "matrixark-native-auto-budget.jsonl")
            scope = {
                "account_id": "acct_native_auto_budget",
                "tenant_id": "tenant_native_auto_budget",
                "user_id": "user_native_auto_budget",
                "session_id": "session_native_auto_budget",
            }
            pack = adapter.retrieve(
                {
                    "scope": scope,
                    "query": "Codex role budget",
                    "max_context_tokens": 100,
                    "ranking": {
                        "source_role_budget_mode": "auto",
                        "memory_layer_budget_mode": "auto",
                        "memory_selection_policy_budget_mode": "auto",
                    },
                    "audit_mode": "off",
                    "debug_context_pack": True,
                }
            )

            self.assertEqual("local-native-pack", pack["pack_id"])
            self.assertEqual(1, len(adapter.native_requests))
            request = adapter.native_requests[0]
            self.assertEqual({"assistant": 42, "tool": 33, "user": 57}, request["source_role_budget_tokens"])
            self.assertEqual("auto", request["source_role_budget_mode"])
            self.assertEqual(
                {
                    "summary": 19,
                    "profile_summary": 28,
                    "same_session_summary": 19,
                    "cross_session_summary": 19,
                    "compression": 23,
                    "profile_compression": 23,
                    "same_session_compression": 19,
                    "cross_session_compression": 19,
                    "pending_async_event": 19,
                    "same_session_event": 42,
                    "cross_session_event": 23,
                    "same_session_segment": 33,
                    "cross_session_segment": 23,
                    "profile_entity": 38,
                },
                request["memory_layer_budget_tokens"],
            )
            self.assertEqual("auto", request["memory_layer_budget_mode"])
            self.assertEqual(
                {
                    "selected_user_prompt": 42,
                    "selected_assistant_decision_outcome_only": 28,
                    "selected_tool_evidence_only": 28,
                },
                request["memory_selection_policy_budget_tokens"],
            )
            self.assertEqual("auto", request["memory_selection_policy_budget_mode"])
            self.assertEqual({"pending_async": 11, "provisional": 23, "final": 66}, request["extraction_phase_budget_tokens"])
            self.assertEqual("auto", request["extraction_phase_budget_mode"])

    def test_codex_outcome_query_auto_budget_prioritizes_assistant_tool_and_profile_memory(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            adapter = NativeCaptureLocalAdapter(Path(tmp_dir) / "matrixark-native-codex-outcome-budget.jsonl")
            scope = {
                "account_id": "acct_native_codex_outcome_budget",
                "tenant_id": "tenant_native_codex_outcome_budget",
                "user_id": "user_native_codex_outcome_budget",
                "session_id": "session_native_codex_outcome_budget",
            }
            pack = adapter.retrieve(
                {
                    "scope": scope,
                    "query": "What did Codex implement, validate, and push last?",
                    "max_context_tokens": 100,
                    "audit_mode": "off",
                    "debug_context_pack": True,
                }
            )

            self.assertEqual("local-native-pack", pack["pack_id"])
            self.assertEqual(1, len(adapter.native_requests))
            request = adapter.native_requests[0]
            self.assertEqual("evidence", request["question_type"])
            self.assertEqual("auto", request["source_role_budget_mode"])
            self.assertEqual({"assistant": 52, "tool": 52, "user": 38}, request["source_role_budget_tokens"])
            self.assertEqual("auto", request["memory_layer_budget_mode"])
            self.assertEqual(55, request["memory_layer_budget_tokens"]["profile_entity"])
            self.assertEqual(33, request["memory_layer_budget_tokens"]["profile_summary"])
            self.assertEqual(36, request["memory_layer_budget_tokens"]["cross_session_event"])
            self.assertEqual(33, request["memory_layer_budget_tokens"]["cross_session_segment"])
            self.assertGreater(
                request["memory_layer_budget_tokens"]["profile_entity"],
                request["memory_layer_budget_tokens"]["same_session_event"],
            )
            self.assertEqual("auto", request["memory_selection_policy_budget_mode"])
            self.assertEqual(
                {
                    "selected_user_prompt": 33,
                    "selected_assistant_decision_outcome_only": 55,
                    "selected_tool_evidence_only": 52,
                    "selected_profile_current_state": 52,
                },
                request["memory_selection_policy_budget_tokens"],
            )

            adapter.native_requests.clear()
            adapter.retrieve(
                {
                    "scope": scope,
                    "query": "What did you implement and validate last?",
                    "max_context_tokens": 100,
                    "audit_mode": "off",
                    "debug_context_pack": True,
                }
            )
            first_person_request = adapter.native_requests[0]
            self.assertEqual("evidence", first_person_request["question_type"])
            self.assertEqual({"assistant": 52, "tool": 52, "user": 38}, first_person_request["source_role_budget_tokens"])
            self.assertEqual(55, first_person_request["memory_layer_budget_tokens"]["profile_entity"])
            self.assertEqual(52, first_person_request["memory_selection_policy_budget_tokens"]["selected_tool_evidence_only"])
            self.assertEqual(
                55,
                first_person_request["memory_selection_policy_budget_tokens"]["selected_assistant_decision_outcome_only"],
            )

    def test_local_native_context_pack_infers_memory_selection_policy_auto_from_related_budget_mode(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            adapter = NativeCaptureLocalAdapter(Path(tmp_dir) / "matrixark-native-inferred-selection-budget.jsonl")
            scope = {
                "account_id": "acct_native_inferred_selection_budget",
                "tenant_id": "tenant_native_inferred_selection_budget",
                "user_id": "user_native_inferred_selection_budget",
                "session_id": "session_native_inferred_selection_budget",
            }
            pack = adapter.retrieve(
                {
                    "scope": scope,
                    "query": "Codex current profile budget",
                    "max_context_tokens": 100,
                    "ranking": {"memory_layer_budget_mode": "auto"},
                    "audit_mode": "off",
                    "debug_context_pack": True,
                }
            )

            self.assertEqual("local-native-pack", pack["pack_id"])
            self.assertEqual(1, len(adapter.native_requests))
            request = adapter.native_requests[0]
            self.assertEqual("auto", request["memory_layer_budget_mode"])
            self.assertEqual("auto", request["memory_selection_policy_budget_mode"])
            self.assertEqual(
                {
                    "selected_user_prompt": 38,
                    "selected_assistant_decision_outcome_only": 42,
                    "selected_tool_evidence_only": 28,
                    "selected_profile_current_state": 47,
                },
                request["memory_selection_policy_budget_tokens"],
            )

    def test_user_goal_query_auto_budget_prioritizes_selected_prompt_and_plan_memory(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            adapter = NativeCaptureLocalAdapter(Path(tmp_dir) / "matrixark-native-user-goal-budget.jsonl")
            scope = {
                "account_id": "acct_native_user_goal_budget",
                "tenant_id": "tenant_native_user_goal_budget",
                "user_id": "user_native_user_goal_budget",
                "session_id": "session_native_user_goal_budget",
            }
            pack = adapter.retrieve(
                {
                    "scope": scope,
                    "query": "What goal did I ask Codex to implement for profile memory retrieval?",
                    "max_context_tokens": 100,
                    "audit_mode": "off",
                    "debug_context_pack": True,
                }
            )

            self.assertEqual("local-native-pack", pack["pack_id"])
            self.assertEqual(1, len(adapter.native_requests))
            request = adapter.native_requests[0]
            self.assertEqual("profile_memory", request["question_type"])
            self.assertEqual("auto", request["source_role_budget_mode"])
            self.assertEqual({"assistant": 33, "tool": 23, "user": 66}, request["source_role_budget_tokens"])
            self.assertEqual("auto", request["memory_selection_policy_budget_mode"])
            self.assertEqual(
                {
                    "selected_user_prompt": 66,
                    "selected_assistant_decision_outcome_only": 28,
                    "selected_tool_evidence_only": 23,
                    "selected_profile_current_state": 52,
                },
                request["memory_selection_policy_budget_tokens"],
            )

    def test_auto_memory_layer_budget_expands_profile_for_current_state_queries(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            adapter = NativeCaptureLocalAdapter(Path(tmp_dir) / "matrixark-native-current-budget.jsonl")
            scope = {
                "account_id": "acct_native_current_budget",
                "tenant_id": "tenant_native_current_budget",
                "user_id": "user_native_current_budget",
                "session_id": "session_native_current_budget",
            }
            pack = adapter.retrieve(
                {
                    "scope": scope,
                    "query": "What is the latest Codex profile preference?",
                    "max_context_tokens": 100,
                    "ranking": {"memory_layer_budget_mode": "auto"},
                    "audit_mode": "off",
                    "debug_context_pack": True,
                }
            )

            self.assertEqual("local-native-pack", pack["pack_id"])
            self.assertEqual(1, len(adapter.native_requests))
            request = adapter.native_requests[0]
            self.assertEqual("current_state", request["question_type"])
            self.assertEqual("auto", request["memory_layer_budget_mode"])
            self.assertEqual("current_state", request["memory_layer_budget_question_type"])
            self.assertIn("prioritize_profile_entity", request["memory_layer_budget_question_reason"])
            self.assertEqual(
                {
                    "summary": 14,
                    "profile_summary": 19,
                    "same_session_summary": 14,
                    "cross_session_summary": 14,
                    "compression": 19,
                    "profile_compression": 23,
                    "same_session_compression": 14,
                    "cross_session_compression": 19,
                    "pending_async_event": 14,
                    "same_session_event": 33,
                    "cross_session_event": 28,
                    "same_session_segment": 28,
                    "cross_session_segment": 28,
                    "profile_entity": 52,
                },
                request["memory_layer_budget_tokens"],
            )
            self.assertGreater(
                request["memory_layer_budget_tokens"]["profile_entity"],
                request["memory_layer_budget_tokens"]["same_session_event"],
            )
            self.assertGreater(
                request["memory_layer_budget_tokens"]["cross_session_event"],
                23,
            )

    def test_auto_memory_layer_budget_expands_profile_for_profile_memory_queries(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            adapter = NativeCaptureLocalAdapter(Path(tmp_dir) / "matrixark-native-profile-memory-budget.jsonl")
            scope = {
                "account_id": "acct_native_profile_memory_budget",
                "tenant_id": "tenant_native_profile_memory_budget",
                "user_id": "user_native_profile_memory_budget",
                "session_id": "session_native_profile_memory_budget",
            }
            pack = adapter.retrieve(
                {
                    "scope": scope,
                    "query": "Show user profile long-term memory and cross-session entities",
                    "max_context_tokens": 100,
                    "ranking": {
                        "source_role_budget_mode": "auto",
                        "memory_layer_budget_mode": "auto",
                        "memory_selection_policy_budget_mode": "auto",
                    },
                    "audit_mode": "off",
                    "debug_context_pack": True,
                }
            )

            self.assertEqual("local-native-pack", pack["pack_id"])
            self.assertEqual(1, len(adapter.native_requests))
            request = adapter.native_requests[0]
            self.assertEqual("profile_memory", request["question_type"])
            self.assertEqual({"assistant": 47, "tool": 42, "user": 47}, request["source_role_budget_tokens"])
            self.assertEqual("auto", request["source_role_budget_mode"])
            self.assertEqual("auto", request["memory_layer_budget_mode"])
            self.assertEqual("profile_memory", request["memory_layer_budget_question_type"])
            self.assertIn("profile_memory_queries_prioritize", request["memory_layer_budget_question_reason"])
            self.assertEqual(61, request["memory_layer_budget_tokens"]["profile_entity"])
            self.assertEqual(47, request["memory_layer_budget_tokens"]["profile_summary"])
            self.assertEqual(42, request["memory_layer_budget_tokens"]["cross_session_summary"])
            self.assertEqual(42, request["memory_layer_budget_tokens"]["profile_compression"])
            self.assertEqual(38, request["memory_layer_budget_tokens"]["cross_session_compression"])
            self.assertEqual(14, request["memory_layer_budget_tokens"]["same_session_compression"])
            self.assertEqual(33, request["memory_layer_budget_tokens"]["cross_session_event"])
            self.assertEqual(33, request["memory_layer_budget_tokens"]["cross_session_segment"])
            self.assertEqual(19, request["memory_layer_budget_tokens"]["pending_async_memory_feature_event"])
            self.assertEqual(33, request["memory_layer_budget_tokens"]["same_session_memory_feature_event"])
            self.assertEqual(23, request["memory_layer_budget_tokens"]["cross_session_memory_feature_event"])
            self.assertEqual(28, request["memory_layer_budget_tokens"]["same_session_memory_feature_segment"])
            self.assertEqual(23, request["memory_layer_budget_tokens"]["cross_session_memory_feature_segment"])
            self.assertGreater(
                request["memory_layer_budget_tokens"]["profile_entity"],
                request["memory_layer_budget_tokens"]["same_session_event"],
            )
            self.assertGreater(
                request["memory_layer_budget_tokens"]["cross_session_event"],
                request["memory_layer_budget_tokens"]["same_session_event"],
            )
            self.assertEqual(
                {
                    "selected_user_prompt": 66,
                    "selected_user_profile_fact": 66,
                    "selected_assistant_profile_fact": 66,
                    "selected_assistant_decision_outcome_only": 19,
                    "selected_tool_evidence_only": 19,
                    "selected_profile_current_state": 52,
                },
                request["memory_selection_policy_budget_tokens"],
            )

    def test_standing_rule_fact_query_uses_profile_memory_budgets(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            adapter = NativeCaptureLocalAdapter(Path(tmp_dir) / "matrixark-native-standing-rule-budget.jsonl")
            scope = {
                "account_id": "acct_native_standing_rule_budget",
                "tenant_id": "tenant_native_standing_rule_budget",
                "user_id": "user_native_standing_rule_budget",
                "session_id": "session_native_standing_rule_budget",
            }
            pack = adapter.retrieve(
                {
                    "scope": scope,
                    "question_type": "fact",
                    "query": "Which repo should you use for TemporalStore builds?",
                    "max_context_tokens": 100,
                    "ranking": {
                        "source_role_budget_mode": "auto",
                        "memory_layer_budget_mode": "auto",
                        "memory_selection_policy_budget_mode": "auto",
                    },
                    "audit_mode": "off",
                    "debug_context_pack": True,
                }
            )

            self.assertEqual("local-native-pack", pack["pack_id"])
            self.assertEqual(1, len(adapter.native_requests))
            request = adapter.native_requests[0]
            self.assertEqual("profile_memory", request["question_type"])
            self.assertEqual("profile_memory", request["memory_layer_budget_question_type"])
            self.assertIn("profile_memory_queries_prioritize", request["memory_layer_budget_question_reason"])
            self.assertEqual(61, request["memory_layer_budget_tokens"]["profile_entity"])
            self.assertGreater(
                request["memory_layer_budget_tokens"]["profile_entity"],
                request["memory_layer_budget_tokens"]["same_session_event"],
            )
            self.assertGreater(
                request["memory_layer_budget_tokens"]["cross_session_event"],
                request["memory_layer_budget_tokens"]["same_session_event"],
            )
            self.assertEqual(66, request["memory_selection_policy_budget_tokens"]["selected_user_profile_fact"])
            self.assertEqual(52, request["memory_selection_policy_budget_tokens"]["selected_profile_current_state"])

    def test_profile_memory_query_defaults_to_bounded_auto_memory_budgets(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            adapter = NativeCaptureLocalAdapter(Path(tmp_dir) / "matrixark-native-profile-memory-default-budget.jsonl")
            scope = {
                "account_id": "acct_native_profile_memory_default_budget",
                "tenant_id": "tenant_native_profile_memory_default_budget",
                "user_id": "user_native_profile_memory_default_budget",
                "session_id": "session_native_profile_memory_default_budget",
            }
            pack = adapter.retrieve(
                {
                    "scope": scope,
                    "query": "Show user profile long-term memory and cross-session entities",
                    "max_context_tokens": 100,
                    "audit_mode": "off",
                    "debug_context_pack": True,
                }
            )

            self.assertEqual("local-native-pack", pack["pack_id"])
            self.assertEqual(1, len(adapter.native_requests))
            request = adapter.native_requests[0]
            self.assertEqual("profile_memory", request["question_type"])
            self.assertEqual("auto", request["source_role_budget_mode"])
            self.assertEqual("auto", request["memory_layer_budget_mode"])
            self.assertEqual("auto", request["memory_selection_policy_budget_mode"])
            self.assertEqual("auto", request["extraction_phase_budget_mode"])
            self.assertEqual({"assistant": 47, "tool": 42, "user": 47}, request["source_role_budget_tokens"])
            self.assertEqual(57, request["memory_layer_budget_tokens"]["profile_entity"])
            self.assertEqual(42, request["memory_layer_budget_tokens"]["profile_summary"])
            self.assertEqual(38, request["memory_layer_budget_tokens"]["cross_session_event"])
            self.assertEqual(61, request["memory_selection_policy_budget_tokens"]["selected_profile_current_state"])
            self.assertEqual(76, request["extraction_phase_budget_tokens"]["final"])
            self.assertTrue(request["cross_session"]["enabled"])
            self.assertEqual(19, request["cross_session"]["budget_tokens"])
            self.assertEqual("profile_memory", request["cross_session"]["question_type"])

    def test_profile_memory_query_rescues_profile_entity_when_single_ref_budget_competes_with_session_event(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            adapter = MatrixArkLocalAdapter(Path(tmp_dir) / "matrixark-profile-memory-bridge-rescue.jsonl")
            base_scope = {
                "account_id": "acct_profile_bridge_rescue",
                "tenant_id": "tenant_profile_bridge_rescue",
                "user_id": "user_profile_bridge_rescue",
            }
            profile_scope = {**base_scope, "session_id": "session_profile_bridge_source"}
            followup_scope = {**base_scope, "session_id": "session_profile_bridge_followup"}
            adapter.append_many(
                [
                    {
                        "record_type": "context_event",
                        "event_id_hash": 4101,
                        "node_hash": 5101,
                        "node_path": [
                            "tenant:tenant_profile_bridge_rescue",
                            "user:user_profile_bridge_rescue",
                            "session:session_profile_bridge_followup",
                        ],
                        "text": "User asks for profile memory. Same-session event is verbose and keyword heavy but only local evidence.",
                        "summary_text": "same-session profile memory query event",
                        "event_type": "user_prompt",
                        "classification": "request",
                        "memory_scope": "session",
                        "session_continuity": "same_session",
                        "source_roles": ["user"],
                        "source_role_counts": {"user": 1},
                        "scope": followup_scope,
                        "updated_at_ms": 2000,
                    },
                    {
                        "record_type": "context_entity",
                        "entity_hash": 4201,
                        "node_hash": 5201,
                        "node_path": [
                            "tenant:tenant_profile_bridge_rescue",
                            "user:user_profile_bridge_rescue",
                            "profile:long_term_memory",
                        ],
                        "entity_type": "assistant_decision",
                        "entity_name": "memory_architecture",
                        "state": "Profile memory should preserve cross-session assistant decisions and tool evidence as durable user-profile entities.",
                        "memory_scope": "user_profile",
                        "session_continuity": "cross_session",
                        "profile_current_state_representative": True,
                        "profile_revision": 1,
                        "source_session_ids": ["session_profile_bridge_source"],
                        "source_entity_hashes": [4200],
                        "source_role": "assistant_response",
                        "source_hook_types": ["hook_boundary"],
                        "source_codex_events": ["Stop"],
                        "scope": profile_scope,
                        "updated_at_ms": 3000,
                    },
                ]
            )

            pack = adapter.retrieve(
                {
                    "scope": followup_scope,
                    "session_scope": "prefer",
                    "query": "Show user profile long-term memory across sessions",
                    "max_context_tokens": 160,
                    "ranking": {"max_selected_refs": 1, "min_similarity_score": 0.0},
                    "audit_mode": "off",
                    "debug_context_pack": True,
                    "include_debug_refs": True,
                }
            )

            self.assertEqual("profile_memory", pack["question_type"])
            self.assertEqual(1, len(pack["selected_refs"]))
            selected = pack["selected_refs"][0]
            self.assertEqual("entity", selected["ref_type"])
            self.assertEqual("user_profile", selected["memory_scope"])
            self.assertEqual("cross_session", selected["session_continuity"])
            self.assertGreaterEqual(
                pack["retrieval_metrics"]["memory_layer_budget"]["by_memory_scope"]["user_profile"]["refs"],
                1,
            )
            self.assertTrue(pack["memory_inventory"]["has_profile_memory"])
            self.assertFalse(pack["memory_inventory"]["profile_records_available_but_not_selected"])

    def test_session_only_candidate_fetch_keeps_durable_profile_bridge_without_cross_session_raw_events(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            adapter = MatrixArkLocalAdapter(Path(tmp_dir) / "matrixark-session-only-profile-bridge.jsonl")
            base_scope = {
                "account_id": "acct_session_only_profile_bridge",
                "tenant_id": "tenant_session_only_profile_bridge",
                "user_id": "user_session_only_profile_bridge",
            }
            identity = {
                **base_scope,
                "session_id": "session_profile_bridge_current",
                "mode": "api_key",
            }
            current_scope = matrixark_mcp_core.enrich_scope_with_identity(
                {**base_scope, "session_id": "session_profile_bridge_current"},
                identity,
            )
            prior_scope = matrixark_mcp_core.enrich_scope_with_identity(
                {**base_scope, "session_id": "session_profile_bridge_prior"},
                {**identity, "session_id": "session_profile_bridge_prior"},
            )
            promoted_profile_scope = {
                "account_id": base_scope["account_id"],
                "tenant_id": base_scope["tenant_id"],
                "user_id": base_scope["user_id"],
            }
            adapter.append_many(
                [
                    {
                        "record_type": "context_event",
                        "event_id_hash": 4411,
                        "node_hash": 5411,
                        "node_path": [
                            "tenant:tenant_session_only_profile_bridge",
                            "user:user_session_only_profile_bridge",
                            "session:session_profile_bridge_current",
                        ],
                        "text": "Current session asks a small factual follow-up about repository location.",
                        "event_type": "user_prompt",
                        "memory_scope": "session",
                        "session_continuity": "same_session",
                        "scope": current_scope,
                        "updated_at_ms": 2000,
                    },
                    {
                        "record_type": "context_event",
                        "event_id_hash": 4412,
                        "node_hash": 5412,
                        "node_path": [
                            "tenant:tenant_session_only_profile_bridge",
                            "user:user_session_only_profile_bridge",
                            "session:session_profile_bridge_prior",
                        ],
                        "text": "Prior raw session event mentions repository location but should not enter session-only retrieval.",
                        "event_type": "user_prompt",
                        "memory_scope": "session",
                        "session_continuity": "cross_session",
                        "scope": prior_scope,
                        "updated_at_ms": 2500,
                    },
                    {
                        "record_type": "context_entity",
                        "entity_hash": 4413,
                        "node_hash": 5413,
                        "node_path": [
                            "tenant:tenant_session_only_profile_bridge",
                            "user:user_session_only_profile_bridge",
                            "profile:long_term_memory",
                        ],
                        "entity_type": "workspace_profile",
                        "entity_name": "temporalstore_repo_location",
                        "state": "TemporalStore work should use /opt/github-services/TemporalStore in Ubuntu.",
                        "memory_scope": "user_profile",
                        "session_continuity": "cross_session",
                        "profile_entity_current": True,
                        "profile_current_state_representative": True,
                        "source_session_ids": ["session_profile_bridge_prior"],
                        "scope": promoted_profile_scope,
                        "access_scope": promoted_profile_scope,
                        "updated_at_ms": 3000,
                    },
                ]
            )

            query_scope = {
                **current_scope,
                "_session_scope": "only",
                "_allow_profile_bridge": True,
            }
            records = adapter.retrieval_records(
                scope=query_scope,
                secondary_index_groups=infer_secondary_index_filter_groups(
                    "TemporalStore repository location",
                    "fact",
                ),
            )["records"]

            returned_ids = {record.get("event_id_hash") or record.get("entity_hash") for record in records}
            self.assertIn(4411, returned_ids)
            self.assertIn(4413, returned_ids)
            self.assertNotIn(4412, returned_ids)
            profile_record = next(record for record in records if record.get("entity_hash") == 4413)
            self.assertEqual("user_profile", profile_record["memory_scope"])
            self.assertEqual("cross_session", profile_record["session_continuity"])

            policy = matrixark_mcp_core.build_cross_session_policy(
                {"query": "TemporalStore repository location", "cross_session": {"enabled": True, "min_entity_bridge_refs": 1}},
                {},
                question_type="fact",
                session_scope="only",
                remote_budget_tokens=160,
            )
            self.assertTrue(policy["enabled"])
            self.assertEqual("allow_durable_profile_bridge_inside_session_only_scope", policy["decision"])

            prefer_policy = matrixark_mcp_core.build_cross_session_policy(
                {"query": "TemporalStore repository location"},
                {},
                question_type="fact",
                session_scope="prefer",
                remote_budget_tokens=160,
            )
            self.assertTrue(prefer_policy["enabled"])
            self.assertEqual(1, prefer_policy["min_entity_bridge_refs"])
            self.assertEqual("always_consider_same_user_cross_session_when_session_scope_prefer", prefer_policy["decision"])

            prefer_pack = adapter.retrieve(
                {
                    "scope": current_scope,
                    "session_scope": "prefer",
                    "question_type": "fact",
                    "query": "TemporalStore repository location",
                    "max_context_tokens": 160,
                    "ranking": {"max_selected_refs": 4, "min_similarity_score": 0.0},
                    "audit_mode": "off",
                }
            )
            prefer_profile_ref = next(
                ref
                for ref in prefer_pack["selected_refs"]
                if ref.get("ref_type") == "entity"
                and ref.get("entity_name") == "temporalstore_repo_location"
            )
            self.assertEqual("user_profile", prefer_profile_ref["memory_scope"])
            self.assertEqual("cross_session", prefer_profile_ref["session_continuity"])

            pack = adapter.retrieve(
                {
                    "scope": current_scope,
                    "session_scope": "only",
                    "question_type": "fact",
                    "query": "TemporalStore repository location",
                    "max_context_tokens": 160,
                    "ranking": {"max_selected_refs": 4, "min_similarity_score": 0.0},
                    "cross_session": {"enabled": True, "min_entity_bridge_refs": 1},
                    "audit_mode": "off",
                    "debug_context_pack": True,
                    "include_debug_refs": True,
                }
            )
            profile_ref = next(
                ref
                for ref in pack["selected_refs"]
                if ref.get("ref_type") == "entity"
                and ref.get("entity_name") == "temporalstore_repo_location"
            )
            self.assertEqual("user_profile", profile_ref["memory_scope"])
            self.assertEqual("cross_session", profile_ref["session_continuity"])
            self.assertIn("/opt/github-services/TemporalStore", profile_ref["text"])
            self.assertFalse(
                any("Prior raw session event" in str(ref.get("text", "")) for ref in pack["selected_refs"])
            )
            self.assertEqual(
                "allow_durable_profile_bridge_inside_session_only_scope",
                pack["recall_policy"]["cross_session"]["decision"],
            )

    def test_normal_query_warns_when_profile_memory_is_available_but_not_selected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            adapter = MatrixArkLocalAdapter(Path(tmp_dir) / "matrixark-profile-memory-not-selected.jsonl")
            base_scope = {
                "account_id": "acct_profile_not_selected",
                "tenant_id": "tenant_profile_not_selected",
                "user_id": "user_profile_not_selected",
            }
            session_scope = {**base_scope, "session_id": "session_profile_not_selected_current"}
            profile_scope = {**base_scope, "session_id": "session_profile_not_selected_prior"}
            adapter.append_many(
                [
                    {
                        "record_type": "context_event",
                        "event_id_hash": 4301,
                        "node_hash": 5301,
                        "node_path": [
                            "tenant:tenant_profile_not_selected",
                            "user:user_profile_not_selected",
                            "session:session_profile_not_selected_current",
                        ],
                        "text": "The current session task is checking the live hook retrieval budget warning path.",
                        "summary_text": "current session retrieval budget task",
                        "event_type": "user_prompt",
                        "classification": "request",
                        "memory_scope": "session",
                        "session_continuity": "same_session",
                        "source_roles": ["user"],
                        "source_role_counts": {"user": 1},
                        "scope": session_scope,
                        "updated_at_ms": 2000,
                    },
                    {
                        "record_type": "context_entity",
                        "entity_hash": 4302,
                        "node_hash": 5302,
                        "node_path": [
                            "tenant:tenant_profile_not_selected",
                            "user:user_profile_not_selected",
                            "profile:long_term_memory",
                        ],
                        "entity_type": "user_preference",
                        "entity_name": "workspace_location",
                        "state": "User profile says TemporalStore work should stay under /opt/github-services.",
                        "memory_scope": "user_profile",
                        "session_continuity": "cross_session",
                        "profile_current_state_representative": True,
                        "source_session_ids": ["session_profile_not_selected_prior"],
                        "source_roles": ["user"],
                        "source_role_counts": {"user": 1},
                        "scope": profile_scope,
                        "updated_at_ms": 3000,
                    },
                ]
            )

            pack = adapter.retrieve(
                {
                    "scope": session_scope,
                    "session_scope": "only",
                    "query": "What is the current session task?",
                    "max_context_tokens": 160,
                    "ranking": {"max_selected_refs": 1, "min_similarity_score": 0.0},
                    "audit_mode": "off",
                }
            )

            self.assertTrue(pack["insufficient_context"])
            self.assertEqual([], pack.get("selected_refs", []))
            self.assertNotIn("memory_inventory", pack)
            self.assertIn("profile_memory_available_but_not_selected", pack["quality_warnings"])
            metrics_pack = adapter.retrieve(
                {
                    "scope": session_scope,
                    "session_scope": "only",
                    "query": "What is the current session task?",
                    "max_context_tokens": 160,
                    "ranking": {"max_selected_refs": 1, "min_similarity_score": 0.0},
                    "audit_mode": "off",
                    "include_retrieval_metrics": True,
                }
            )
            metrics_inventory = metrics_pack["retrieval_metrics"]["memory_inventory"]
            self.assertTrue(metrics_inventory["has_profile_memory"])
            self.assertTrue(metrics_inventory["profile_records_available_but_not_selected"])

    def test_profile_memory_query_selects_existing_profile_summary_without_refresh(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            adapter = MatrixArkLocalAdapter(Path(tmp_dir) / "matrixark-profile-summary-existing.jsonl")
            scope = {
                "account_id": "acct_profile_summary_existing",
                "tenant_id": "tenant_profile_summary_existing",
                "user_id": "user_profile_summary_existing",
                "session_id": "session_profile_summary_source",
            }
            followup_scope = {**scope, "session_id": "session_profile_summary_followup"}
            adapter.append_many(
                [
                    {
                        "record_type": "context_entity",
                        "entity_hash": 4301,
                        "node_hash": 5301,
                        "node_path": [
                            "tenant:tenant_profile_summary_existing",
                            "user:user_profile_summary_existing",
                            "profile:long_term_memory",
                        ],
                        "entity_type": "assistant_decision",
                        "entity_name": "summary_runtime",
                        "state": "Profile summaries should be returned with profile entities for long-term Codex memory queries.",
                        "memory_scope": "user_profile",
                        "session_continuity": "cross_session",
                        "profile_current_state_representative": True,
                        "profile_revision": 1,
                        "source_session_ids": ["session_profile_summary_source"],
                        "source_entity_hashes": [4300],
                        "source_roles": ["assistant"],
                        "source_role_counts": {"assistant": 1},
                        "scope": scope,
                        "updated_at_ms": 3000,
                    },
                    {
                        "record_type": "context_summary",
                        "summary_hash": 4401,
                        "node_hash": 5301,
                        "node_path": [
                            "tenant:tenant_profile_summary_existing",
                            "user:user_profile_summary_existing",
                            "profile:long_term_memory",
                        ],
                        "summary_type": "node_l0",
                        "summary_text": "Profile summary: Codex long-term memory keeps assistant decisions and tool evidence across sessions.",
                        "memory_scope": "user_profile",
                        "session_continuity": "cross_session",
                        "source_roles": ["assistant", "tool"],
                        "source_role_counts": {"assistant": 1, "tool": 1},
                        "source_entity_types": ["assistant_decision", "tool_evidence"],
                        "scope": scope,
                        "updated_at_ms": 3100,
                    },
                ]
            )

            pack = adapter.retrieve(
                {
                    "scope": followup_scope,
                    "session_scope": "prefer",
                    "query": "Show user profile long-term memory across sessions",
                    "max_context_tokens": 220,
                    "ranking": {
                        "max_selected_refs": 2,
                        "min_similarity_score": 0.0,
                        "pre_retrieval_summary_refresh": False,
                    },
                    "audit_mode": "off",
                    "debug_context_pack": True,
                    "include_debug_refs": True,
                }
            )

            selected_layers = {
                (ref.get("ref_type"), ref.get("memory_scope"), ref.get("session_continuity"))
                for ref in pack["selected_refs"]
            }
            self.assertIn(("entity", "user_profile", "cross_session"), selected_layers)
            self.assertIn(("summary", "user_profile", "cross_session"), selected_layers)
            budget = pack["retrieval_metrics"]["memory_layer_budget"]["by_memory_layer"]
            self.assertGreaterEqual(budget["profile_entity"]["refs"], 1)
            self.assertGreaterEqual(budget["profile_summary"]["refs"], 1)
            self.assertFalse(pack["pre_retrieval_summary_refresh"]["enabled"])

    def test_current_state_query_selects_existing_profile_summary_without_refresh(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            adapter = MatrixArkLocalAdapter(Path(tmp_dir) / "matrixark-current-profile-summary-existing.jsonl")
            scope = {
                "account_id": "acct_current_profile_summary_existing",
                "tenant_id": "tenant_current_profile_summary_existing",
                "user_id": "user_current_profile_summary_existing",
                "session_id": "session_current_profile_summary_source",
            }
            followup_scope = {**scope, "session_id": "session_current_profile_summary_followup"}
            adapter.append_many(
                [
                    {
                        "record_type": "context_entity",
                        "entity_hash": 4501,
                        "node_hash": 5501,
                        "node_path": [
                            "tenant:tenant_current_profile_summary_existing",
                            "user:user_current_profile_summary_existing",
                            "profile:long_term_memory",
                        ],
                        "entity_type": "assistant_decision",
                        "entity_name": "summary_runtime",
                        "state": "Current retrieval should return durable profile summaries with profile entities.",
                        "memory_scope": "user_profile",
                        "session_continuity": "cross_session",
                        "profile_current_state_representative": True,
                        "profile_revision": 1,
                        "source_session_ids": ["session_current_profile_summary_source"],
                        "source_entity_hashes": [4500],
                        "source_roles": ["assistant"],
                        "source_role_counts": {"assistant": 1},
                        "scope": scope,
                        "updated_at_ms": 3000,
                    },
                    {
                        "record_type": "context_summary",
                        "summary_hash": 4601,
                        "node_hash": 5501,
                        "node_path": [
                            "tenant:tenant_current_profile_summary_existing",
                            "user:user_current_profile_summary_existing",
                            "profile:long_term_memory",
                        ],
                        "summary_type": "node_l0",
                        "summary_text": "Profile summary: current retrieval returns durable profile summaries with profile entities.",
                        "memory_scope": "user_profile",
                        "session_continuity": "cross_session",
                        "profile_summary_current": True,
                        "source_roles": ["assistant"],
                        "source_role_counts": {"assistant": 1},
                        "source_entity_types": ["assistant_decision"],
                        "scope": scope,
                        "updated_at_ms": 3100,
                    },
                ]
            )

            pack = adapter.retrieve(
                {
                    "scope": followup_scope,
                    "session_scope": "prefer",
                    "question_type": "current_state",
                    "query": "What is the current profile summary retrieval runtime?",
                    "max_context_tokens": 220,
                    "ranking": {
                        "max_selected_refs": 2,
                        "min_similarity_score": 0.0,
                        "pre_retrieval_summary_refresh": False,
                    },
                    "audit_mode": "off",
                    "debug_context_pack": True,
                    "include_debug_refs": True,
                }
            )

            selected_layers = {
                (ref.get("ref_type"), ref.get("memory_scope"), ref.get("session_continuity"))
                for ref in pack["selected_refs"]
            }
            self.assertIn(("entity", "user_profile", "cross_session"), selected_layers)
            self.assertIn(("summary", "user_profile", "cross_session"), selected_layers)
            budget = pack["retrieval_metrics"]["memory_layer_budget"]["by_memory_layer"]
            self.assertGreaterEqual(budget["profile_entity"]["refs"], 1)
            self.assertGreaterEqual(budget["profile_summary"]["refs"], 1)
            self.assertFalse(pack["pre_retrieval_summary_refresh"]["enabled"])

    def test_invalid_pre_retrieval_summary_refresh_limit_env_does_not_break_import(self) -> None:
        env = os.environ.copy()
        env["PYTHONPATH"] = "tools"
        env["MATRIXARK_PRE_RETRIEVAL_SUMMARY_REFRESH_LIMIT"] = "not-an-int"
        result = subprocess.run(
            [
                sys.executable,
                "-c",
                "import matrixark_mcp_local_adapter as m; print(m.PRE_RETRIEVAL_SUMMARY_REFRESH_LIMIT)",
            ],
            cwd=Path(__file__).resolve().parents[1],
            env=env,
            check=True,
            text=True,
            capture_output=True,
        )
        self.assertEqual("2", result.stdout.strip())

    def test_pre_retrieval_summary_refresh_derives_balanced_layer_budget(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            adapter = NativeCaptureLocalAdapter(Path(tmp_dir) / "matrixark-native-pre-refresh-budget.jsonl")
            scope = {
                "account_id": "acct_native_pre_refresh_budget",
                "tenant_id": "tenant_native_pre_refresh_budget",
                "user_id": "user_native_pre_refresh_budget",
                "session_id": "session_native_pre_refresh_budget",
            }
            pack = adapter.retrieve(
                {
                    "scope": scope,
                    "query": "plain memory lookup budget",
                    "max_context_tokens": 100,
                    "pre_retrieval_summary_refresh": True,
                    "audit_mode": "off",
                    "debug_context_pack": True,
                }
            )

            self.assertEqual("local-native-pack", pack["pack_id"])
            self.assertEqual(1, len(adapter.native_requests))
            request = adapter.native_requests[0]
            self.assertEqual(
                "pre_retrieval_summary_refresh_balanced",
                request["memory_layer_budget_mode"],
            )
            self.assertEqual(14, request["memory_layer_budget_tokens"]["summary"])
            self.assertEqual(28, request["memory_layer_budget_tokens"]["profile_summary"])
            self.assertEqual(23, request["memory_layer_budget_tokens"]["profile_compression"])
            self.assertEqual(23, request["memory_layer_budget_tokens"]["cross_session_compression"])
            self.assertEqual(42, request["memory_layer_budget_tokens"]["profile_entity"])
            self.assertGreater(
                request["memory_layer_budget_tokens"]["profile_entity"],
                request["memory_layer_budget_tokens"]["summary"],
            )
            auto_pack = adapter.retrieve(
                {
                    "scope": scope,
                    "query": "plain memory lookup budget with auto mode",
                    "max_context_tokens": 100,
                    "pre_retrieval_summary_refresh": True,
                    "ranking": {"memory_layer_budget_mode": "auto"},
                    "audit_mode": "off",
                    "debug_context_pack": True,
                }
            )
            self.assertEqual("local-native-pack", auto_pack["pack_id"])
            self.assertEqual(2, len(adapter.native_requests))
            auto_request = adapter.native_requests[1]
            self.assertEqual(
                "pre_retrieval_summary_refresh_balanced",
                auto_request["memory_layer_budget_mode"],
            )
            self.assertEqual(14, auto_request["memory_layer_budget_tokens"]["summary"])
            self.assertEqual(28, auto_request["memory_layer_budget_tokens"]["profile_summary"])
            self.assertEqual(23, auto_request["memory_layer_budget_tokens"]["profile_compression"])
            self.assertEqual(23, auto_request["memory_layer_budget_tokens"]["cross_session_compression"])
            self.assertEqual(42, auto_request["memory_layer_budget_tokens"]["profile_entity"])
            profile_pack = adapter.retrieve(
                {
                    "scope": scope,
                    "query": "show user profile long term memory across sessions",
                    "max_context_tokens": 100,
                    "pre_retrieval_summary_refresh": True,
                    "ranking": {"memory_layer_budget_mode": "auto"},
                    "audit_mode": "off",
                    "debug_context_pack": True,
                }
            )
            self.assertEqual("local-native-pack", profile_pack["pack_id"])
            self.assertEqual(3, len(adapter.native_requests))
            profile_request = adapter.native_requests[2]
            self.assertEqual("profile_memory", profile_request["question_type"])
            self.assertEqual(
                "pre_retrieval_summary_refresh_profile_memory",
                profile_request["memory_layer_budget_mode"],
            )
            self.assertEqual(47, profile_request["memory_layer_budget_tokens"]["profile_summary"])
            self.assertEqual(61, profile_request["memory_layer_budget_tokens"]["profile_entity"])
            self.assertEqual(33, profile_request["memory_layer_budget_tokens"]["cross_session_event"])
            self.assertEqual(52, profile_request["memory_layer_budget_tokens"]["pending_async_memory_feature_event"])
            self.assertEqual(52, profile_request["memory_layer_budget_tokens"]["same_session_memory_feature_event"])
            self.assertEqual(66, profile_request["memory_layer_budget_tokens"]["cross_session_memory_feature_event"])
            self.assertEqual(47, profile_request["memory_layer_budget_tokens"]["same_session_memory_feature_segment"])
            self.assertEqual(64, profile_request["memory_layer_budget_tokens"]["cross_session_memory_feature_segment"])
            self.assertGreater(
                profile_request["memory_layer_budget_tokens"]["profile_entity"],
                profile_request["memory_layer_budget_tokens"]["same_session_event"],
            )
            explicit_pack = adapter.retrieve(
                {
                    "scope": scope,
                    "query": "plain memory lookup budget with explicit tokens",
                    "max_context_tokens": 100,
                    "pre_retrieval_summary_refresh": True,
                    "memory_layer_budget_tokens": {"profile_summary": 7, "profile_entity": 11},
                    "audit_mode": "off",
                    "debug_context_pack": True,
                }
            )
            self.assertEqual("local-native-pack", explicit_pack["pack_id"])
            self.assertEqual(4, len(adapter.native_requests))
            explicit_request = adapter.native_requests[3]
            self.assertEqual("explicit", explicit_request["memory_layer_budget_mode"])
            self.assertEqual(
                {"profile_summary": 7, "profile_entity": 11},
                explicit_request["memory_layer_budget_tokens"],
            )

    def test_context_pack_cache_key_includes_source_role_budget_tokens(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            adapter = MatrixArkLocalAdapter(Path(tmp_dir) / "matrixark-source-role-cache.jsonl")
            scope = {
                "account_id": "acct_source_role_cache",
                "tenant_id": "tenant_source_role_cache",
                "user_id": "user_source_role_cache",
                "session_id": "session_source_role_cache",
            }

            first = adapter.retrieve(
                {
                    "scope": scope,
                    "query": "GPU budget",
                    "max_context_tokens": 256,
                    "source_role_budget_tokens": {"assistant": 128},
                    "ranking": {
                        "max_selected_refs": 8,
                        "min_similarity_score": 0.0,
                        "budget_fill_policy": "force_fill",
                    },
                    "audit_mode": "off",
                    "debug_context_pack": True,
                }
            )
            second = adapter.retrieve(
                {
                    "scope": scope,
                    "query": "GPU budget",
                    "max_context_tokens": 256,
                    "source_role_budget_tokens": {"assistant": 1},
                    "ranking": {"max_selected_refs": 4, "min_similarity_score": 0.0},
                    "audit_mode": "off",
                    "debug_context_pack": True,
                }
            )

            self.assertFalse(first.get("context_pack_cache_hit", False))
            self.assertFalse(second.get("context_pack_cache_hit", False))

    def test_context_pack_cache_key_includes_memory_layer_budget_tokens(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            adapter = MatrixArkLocalAdapter(Path(tmp_dir) / "matrixark-memory-layer-cache.jsonl")
            scope = {
                "account_id": "acct_memory_layer_cache",
                "tenant_id": "tenant_memory_layer_cache",
                "user_id": "user_memory_layer_cache",
                "session_id": "session_memory_layer_cache",
            }

            first = adapter.retrieve(
                {
                    "scope": scope,
                    "query": "summary budget",
                    "max_context_tokens": 256,
                    "memory_layer_budget_tokens": {"summary": 128},
                    "ranking": {"max_selected_refs": 4, "min_similarity_score": 0.0},
                    "audit_mode": "off",
                    "debug_context_pack": True,
                }
            )
            second = adapter.retrieve(
                {
                    "scope": scope,
                    "query": "summary budget",
                    "max_context_tokens": 256,
                    "memory_layer_budget_tokens": {"summary": 1},
                    "ranking": {"max_selected_refs": 4, "min_similarity_score": 0.0},
                    "audit_mode": "off",
                    "debug_context_pack": True,
                }
            )

            self.assertFalse(first.get("context_pack_cache_hit", False))
            self.assertFalse(second.get("context_pack_cache_hit", False))

    def test_context_pack_cache_key_includes_auto_memory_selection_policy_mode(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            adapter = MatrixArkLocalAdapter(Path(tmp_dir) / "matrixark-selection-policy-auto-cache.jsonl")
            scope = {
                "account_id": "acct_selection_policy_auto_cache",
                "tenant_id": "tenant_selection_policy_auto_cache",
                "user_id": "user_selection_policy_auto_cache",
                "session_id": "session_selection_policy_auto_cache",
            }

            first = adapter.retrieve(
                {
                    "scope": scope,
                    "query": "assistant decision budget",
                    "max_context_tokens": 256,
                    "ranking": {
                        "max_selected_refs": 4,
                        "min_similarity_score": 0.0,
                        "memory_selection_policy_budget_mode": "auto",
                    },
                    "audit_mode": "off",
                    "debug_context_pack": True,
                }
            )
            second = adapter.retrieve(
                {
                    "scope": scope,
                    "query": "assistant decision budget",
                    "max_context_tokens": 256,
                    "ranking": {"max_selected_refs": 4, "min_similarity_score": 0.0},
                    "audit_mode": "off",
                    "debug_context_pack": True,
                }
            )

            self.assertFalse(first.get("context_pack_cache_hit", False))
            self.assertFalse(second.get("context_pack_cache_hit", False))
            self.assertIn(
                first["recall_policy"]["memory_selection_policy_budget_policy"]["mode"],
                {"auto", "disabled"},
            )
            self.assertIn(
                second["recall_policy"]["memory_selection_policy_budget_policy"]["mode"],
                {"auto", "disabled"},
            )

    def test_context_pack_cache_key_includes_memory_selection_policy_budget_tokens(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            adapter = MatrixArkLocalAdapter(Path(tmp_dir) / "matrixark-selection-policy-cache.jsonl")
            scope = {
                "account_id": "acct_selection_policy_cache",
                "tenant_id": "tenant_selection_policy_cache",
                "user_id": "user_selection_policy_cache",
                "session_id": "session_selection_policy_cache",
            }

            first = adapter.retrieve(
                {
                    "scope": scope,
                    "query": "assistant decision budget",
                    "max_context_tokens": 256,
                    "memory_selection_policy_budget_tokens": {"selected_assistant_decision_outcome_only": 128},
                    "ranking": {"max_selected_refs": 4, "min_similarity_score": 0.0},
                    "audit_mode": "off",
                    "debug_context_pack": True,
                }
            )
            second = adapter.retrieve(
                {
                    "scope": scope,
                    "query": "assistant decision budget",
                    "max_context_tokens": 256,
                    "memory_selection_policy_budget_tokens": {"selected_assistant_decision_outcome_only": 1},
                    "ranking": {"max_selected_refs": 4, "min_similarity_score": 0.0},
                    "audit_mode": "off",
                    "debug_context_pack": True,
                }
            )

            self.assertFalse(first.get("context_pack_cache_hit", False))
            self.assertFalse(second.get("context_pack_cache_hit", False))

    def test_context_pack_cache_key_includes_extraction_phase_budget_tokens(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            adapter = MatrixArkLocalAdapter(Path(tmp_dir) / "matrixark-extraction-phase-cache.jsonl")
            scope = {
                "account_id": "acct_extraction_phase_cache",
                "tenant_id": "tenant_extraction_phase_cache",
                "user_id": "user_extraction_phase_cache",
                "session_id": "session_extraction_phase_cache",
            }

            first = adapter.retrieve(
                {
                    "scope": scope,
                    "query": "phase budget",
                    "max_context_tokens": 256,
                    "extraction_phase_budget_tokens": {"provisional": 128},
                    "ranking": {"max_selected_refs": 4, "min_similarity_score": 0.0},
                    "audit_mode": "off",
                    "debug_context_pack": True,
                }
            )
            second = adapter.retrieve(
                {
                    "scope": scope,
                    "query": "phase budget",
                    "max_context_tokens": 256,
                    "extraction_phase_budget_tokens": {"provisional": 1},
                    "ranking": {"max_selected_refs": 4, "min_similarity_score": 0.0},
                    "audit_mode": "off",
                    "debug_context_pack": True,
                }
            )

            self.assertFalse(first.get("context_pack_cache_hit", False))
            self.assertFalse(second.get("context_pack_cache_hit", False))

    def test_hot_ingest_embeddings_preserve_serving_layer_lineage(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            adapter = MatrixArkLocalAdapter(Path(tmp_dir) / "matrixark-hot-embedding-lineage.jsonl")
            scope = {
                "account_id": "acct_hot_embedding",
                "tenant_id": "tenant_hot_embedding",
                "user_id": "user_hot_embedding",
                "session_id": "session_hot_embedding",
            }
            adapter.ingest(
                {
                    "scope": scope,
                    "metadata": {
                        "codex_memory_selection": {
                            "policy": "selected_assistant_decision_outcome_only",
                            "source_role": "assistant",
                            "codex_event": "Stop",
                            "selected_text_chars": 42,
                            "selected_line_count": 1,
                            "original_text_chars": 420,
                            "original_line_count": 10,
                            "dropped_text_chars": 378,
                            "dropped_line_count": 9,
                            "selection_lossy": True,
                        }
                    },
                    "messages": [
                        {
                            "role": "assistant",
                            "content": "Decision: keep hot embeddings recoverable for profile/session budgets.",
                        }
                    ],
                },
                hook={
                    "source": "codex",
                    "hook_type": "after_llm",
                    "hook_id": "Stop:hot-embedding-lineage",
                    "observed_at_ms": 1000,
                    "idempotency_key": "hot-embedding-lineage",
                    "trigger": "Stop",
                    "auto_captured": True,
                },
            )

            # Folded: the owners carry the vectors, and the retired records' lineage rides
            # along under embedding_meta -- one meta per owner kind, exactly the fields the
            # separate records used to persist.
            rows = adapter.read_all()
            event_owner = next(
                record for record in rows
                if record.get("record_type") == "context_event" and record.get("vector")
            )
            summary_owner = next(
                record for record in rows
                if record.get("record_type") == "context_summary"
                and record.get("summary_type") == "session_l0"
                and record.get("vector")
            )
            event_embedding = event_owner.get("embedding_meta") or {}
            session_summary_embedding = summary_owner.get("embedding_meta") or {}
            embeddings = [event_embedding, session_summary_embedding]
            self.assertEqual("assistant_response", event_embedding["event_type"])
            self.assertEqual("NEW_EVENT", event_embedding["classification"])
            self.assertEqual("observed", event_embedding["status"])
            self.assertEqual("message", event_embedding["source_kind"])
            hot_event = next(record for record in rows if record.get("record_type") == "context_event")
            self.assertEqual("assistant_response", hot_event["event_type"])
            event_indexes = {
                record.get("index_name")
                for record in adapter.read_all()
                if record.get("record_type") == "context_index"
                and record.get("data_model") == "context_event"
            }
            self.assertIn("event_type:assistant_response", event_indexes)
            self.assertNotIn("event_type", session_summary_embedding)
            for embedding in embeddings:
                self.assertEqual("session", embedding["memory_scope"])
                self.assertEqual("same_session", embedding["session_continuity"])
                self.assertEqual(["session"], embedding["source_memory_scopes"])
                self.assertEqual(["same_session"], embedding["source_session_continuities"])
                self.assertEqual(["hot_path"], embedding["source_extraction_phases"])
                self.assertEqual(["assistant"], embedding["source_roles"])
                self.assertEqual({"assistant": 1}, embedding["source_role_counts"])
                self.assertEqual(["after_llm"], embedding["source_hook_types"])
                self.assertEqual({"after_llm": 1}, embedding["source_hook_type_counts"])
                self.assertEqual(["Stop"], embedding["source_codex_events"])
                self.assertEqual({"Stop": 1}, embedding["source_codex_event_counts"])
                self.assertEqual(
                    ["selected_assistant_decision_outcome_only"],
                    embedding["source_memory_selection_policies"],
                )
                self.assertEqual(
                    {"selected_assistant_decision_outcome_only": 1},
                    embedding["source_memory_selection_policy_counts"],
                )
                self.assertNotIn("source_event_ids", embedding)
                self.assertNotIn("source_session_ids", embedding)

