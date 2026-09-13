#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""The two hooks keep private copies of the idle-commit spawn, and they disagree about when.

`spawn_idle_commit_worker_child` launches a DETACHED python process that will commit a session
buffer when it goes idle. `matrixark_agent_hook` and `matrixark_codex_hook` each define one, both
are entry points, and both call their own copy from their own live path. The bodies differ, and
the first difference decides whether a process is started at all.

MEASURED BY CALLING BOTH, with `subprocess.Popen` replaced by a recorder so nothing is spawned.

1. ONLY THE CODEX HOOK ASKS WHAT SCHEDULED THE BATCH.

   The Codex copy has

       if auto_batch and auto_batch.get("trigger_policy") != "idle_timeout":
           return {"status": "skipped", "reason": "scheduled_trigger_is_not_idle_timeout"}

   and the agent copy has no such statement. Given a session buffer with
   `idle_commit_scheduled` true whose `auto_batch_extract_result` says the trigger was
   `message_threshold`, the Codex hook returns `skipped` and starts nothing; the agent hook
   returns `spawned` and starts a detached worker. The two existing checks that touch this
   function -- `test_matrixark_popular_agent_hooks` and `test_codex_pipeline_part3` -- each
   exercise their own hook with `trigger_policy` ALREADY `idle_timeout`, so neither reaches the
   branch where they differ.

2. THE AGENT COPY IGNORES AN ARGUMENT IT REQUIRES.

   `session_id_source` is a required keyword argument of both. The Codex copy spends it:

       if session_id_source:
           cmd.extend(["--query", f"idle commit worker for {session_id_source}"])

   The agent copy names it in the signature and never reads it. Measured: two different
   `session_id_source` values produce a BYTE-IDENTICAL child command through the agent hook and
   different ones through the Codex hook. `--query` is not an option the agent hook lacks -- its
   own parser defines it, and it is the only option the agent parser defines that the Codex spawn
   forwards and the agent spawn does not, so the child is started without a value the parent could
   have supplied.

3. A FAILED SPAWN IS RECORDED DIFFERENTLY.

   On `OSError` the agent copy stores `str(exc)[:300]` and the Codex copy stores
   `_compact_one_line(str(exc), max_chars=300)`. A multi-line error keeps its newlines and runs of
   spaces in one record and is folded to a single line in the other.

WHAT IS NOT THE DIVERGENCE, having checked rather than assumed:

  * The ENVIRONMENT. Both copies read exactly one name, `MATRIXARK_DISABLE_IDLE_COMMIT_WORKER`,
    and write exactly the same three into the child. There is a separate finding that some hook
    controls reach the Codex hook and not the agent hook; this is not an instance of it, and a
    test below pins the two environment surfaces equal so that if one copy ever does start
    reading a control the other does not, it fails HERE and is not filed under the wrong cause.
  * The CLI-VALUE HELPER. `append_cli_value` and `_append_cli_value` are different function
    objects, which is enough to make a body hash differ. They agree on every value a hook passes,
    including the ones that decide whether a flag is emitted at all, and a test below says so.
  * `--agent`, and the other options each spawn forwards alone. Of the eight the Codex spawn
    forwards and the agent spawn does not, seven are not options the agent hook defines; of the
    one the agent forwards alone, the Codex hook has no such option. Two entry points with
    different command lines are not drift. `--query` is the exception, and it is item 2.

THIS FILE DOES NOT ASSERT THAT THE COPIES AGREE. Adding the trigger check to the agent hook stops
it starting workers it starts today; passing the session source changes the child's command line;
folding the error text changes a stored record. Each is a decision, not a cleanup. The divergence
is RECORDED in both directions: a copy that stops diverging fails here and asks which side won.
"""
from __future__ import annotations

import argparse
import ast
import hashlib
import importlib
import os
import subprocess
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))

AGENT = "matrixark_agent_hook"
CODEX = "matrixark_codex_hook"
FUNCTION = "spawn_idle_commit_worker_child"

#: Floors on the scan, so a renamed or unparseable module fails loudly instead of quietly
#: comparing nothing.
DEFINITION_FLOOR = {AGENT: 30, CODEX: 120}

#: The only environment name either copy reads, and the three it writes into the child.
RECORDED_ENV_READS = {"MATRIXARK_DISABLE_IDLE_COMMIT_WORKER"}
RECORDED_ENV_WRITES = {
    "MATRIXARK_IDLE_COMMIT_CUTOFF_MS",
    "MATRIXARK_IDLE_COMMIT_WORKER_DELAY_MS",
    "MATRIXARK_IDLE_COMMIT_WORKER_PARENT_EVENT",
}


def _import(stem):
    try:
        return importlib.import_module("tools." + stem)
    except ImportError:  # Direct script execution from tools/.
        return importlib.import_module(stem)


def _tree(stem):
    with open(os.path.join(TOOLS, stem + ".py"), encoding="utf-8", errors="replace") as handle:
        return ast.parse(handle.read())


def _definition(stem, name=FUNCTION):
    for node in _tree(stem).body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == name:
            return node
    return None


def _body_digest(node):
    body = node.body
    if (body and isinstance(body[0], ast.Expr) and isinstance(body[0].value, ast.Constant)
            and isinstance(body[0].value.value, str)):
        body = body[1:]  # prose is not behaviour
    return hashlib.sha256(
        ast.dump(ast.Module(body=body, type_ignores=[])).encode("utf-8")).hexdigest()[:12]


def _environment_surface(stem):
    """(names read, names written) inside this module's copy of the spawn."""
    node = _definition(stem)
    reads, writes = set(), set()
    if node is None:
        return reads, writes
    for inner in ast.walk(node):
        if (isinstance(inner, ast.Call) and isinstance(inner.func, ast.Attribute)
                and inner.func.attr in {"get", "getenv"} and inner.args
                and isinstance(inner.args[0], ast.Constant)
                and isinstance(inner.args[0].value, str)
                and inner.args[0].value.isupper()):
            reads.add(inner.args[0].value)
        if (isinstance(inner, ast.Subscript) and isinstance(inner.slice, ast.Constant)
                and isinstance(inner.slice.value, str) and inner.slice.value.isupper()):
            target = writes if isinstance(inner.ctx, ast.Store) else reads
            target.add(inner.slice.value)
    return reads, writes


def _parser_options(stem):
    """Every long option this module's argument parser defines."""
    out = set()
    for node in ast.walk(_tree(stem)):
        if (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                and node.func.attr == "add_argument"):
            for argument in node.args:
                if (isinstance(argument, ast.Constant) and isinstance(argument.value, str)
                        and argument.value.startswith("--")):
                    out.add(argument.value)
    return out


def _args(**overrides):
    """A namespace that reaches the spawn. Every field the two copies read is present."""
    namespace = argparse.Namespace(
        idle_commit_worker_only=False,
        idle_commit_timeout_ms=5000,
        agent="generic", backend="local",
        account_id="a", tenant_id="t", user_id="u", session_id="s",
        team="team", project="proj", session_commit_threshold=20,
        understanding_provider="rules", segment_provider="deterministic",
        request_timeout_ms=60000, io_timeout_ms=60000,
        repo_root="/tmp", event="Stop",
        api_key="", metaserver="", namespace="", table="",
        temporalstore_lib="", rust_proxy="", rust_direct_sdk="", rust_cli="",
        storage_prefix="", session_state_dir="", event_log="",
        extraction_provider="", segment_model="", segment_model_path="",
        segment_max_new_tokens="", segment_provider_fallback="",
        skip_prior_context=False, query="",
    )
    for key, value in overrides.items():
        setattr(namespace, key, value)
    return namespace


def _ingest(trigger_policy):
    return {
        "session_buffer": {
            "idle_commit_scheduled": True,
            "idle_commit_deadline_ms": 0,
            "idle_commit_cutoff_ms": 111,
        },
        "auto_batch_extract_result": {"trigger_policy": trigger_policy},
    }


class _Recorder:
    """Stands in for subprocess.Popen. Nothing is started."""

    def __init__(self, calls, explode=None):
        self.calls = calls
        self.explode = explode

    def __call__(self, cmd, **kwargs):
        if self.explode is not None:
            raise self.explode
        self.calls.append((list(cmd), kwargs.get("env") or {}))
        return self


def _spawn(stem, *, trigger_policy, session_id_source, explode=None):
    """Call one copy and return (its result, the child command or None)."""
    module = _import(stem)
    calls = []
    real = subprocess.Popen
    subprocess.Popen = _Recorder(calls, explode)
    try:
        result = module.spawn_idle_commit_worker_child(
            _args(), ingest=_ingest(trigger_policy), session_id_source=session_id_source)
    finally:
        subprocess.Popen = real
    return result, (calls[0][0] if calls else None)


class TheIdleCommitSpawnHasOneDefinition(unittest.TestCase):

    def test_the_pair_is_there_to_compare(self) -> None:
        """A floor. Every assertion below passes over an empty read."""
        for stem in (AGENT, CODEX):
            with self.subTest(module=stem):
                definitions = [node.name for node in _tree(stem).body
                               if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))]
                self.assertGreaterEqual(
                    len(definitions), DEFINITION_FLOOR[stem],
                    "%s contributed %d top-level functions, under the floor of %d -- a renamed, "
                    "moved or unparseable module makes every comparison here vacuous"
                    % (stem, len(definitions), DEFINITION_FLOOR[stem]))
                self.assertIsNotNone(
                    _definition(stem),
                    "%s no longer defines %s. If the two were consolidated, strike this file and "
                    "say which copy won" % (stem, FUNCTION))

    def test_the_two_bodies_still_differ(self) -> None:
        """The record itself, in the direction that fires when somebody picks a winner."""
        self.assertNotEqual(
            _body_digest(_definition(AGENT)), _body_digest(_definition(CODEX)),
            "the two copies of %s now have the same body. That is the fix -- delete this file "
            "and the record with it" % FUNCTION)

    def test_only_one_hook_asks_what_scheduled_the_batch(self) -> None:
        """The difference that decides whether a detached process is started at all."""
        agent_result, agent_cmd = _spawn(
            AGENT, trigger_policy="message_threshold", session_id_source="sess-1")
        codex_result, codex_cmd = _spawn(
            CODEX, trigger_policy="message_threshold", session_id_source="sess-1")

        self.assertEqual(
            "spawned", agent_result.get("status"),
            "the agent hook no longer starts a worker for a schedule that was not an idle "
            "timeout. If it grew the trigger check, strike this record and say so")
        self.assertIsNotNone(agent_cmd, "the agent hook reported spawned and started nothing")
        self.assertEqual(
            "skipped", codex_result.get("status"),
            "the Codex hook no longer skips a schedule that was not an idle timeout")
        self.assertEqual(
            "scheduled_trigger_is_not_idle_timeout", codex_result.get("reason"),
            "the Codex hook skipped for a different reason than the trigger check, so this "
            "fixture is no longer separating the two copies")
        self.assertIsNone(codex_cmd, "the Codex hook reported skipped and started something")

    def test_the_fixture_reaches_the_spawn_on_both_sides(self) -> None:
        """The control for the test above, and the floor on the FIXTURE rather than the code.

        With the trigger the Codex copy wants, BOTH spawn. Without this, a fixture that failed an
        earlier check for some unrelated reason -- an unscheduled buffer, a disabled timeout --
        would produce the same asymmetry and this file would be recording nothing.
        """
        for stem in (AGENT, CODEX):
            with self.subTest(module=stem):
                result, cmd = _spawn(
                    stem, trigger_policy="idle_timeout", session_id_source="sess-1")
                self.assertEqual(
                    "spawned", result.get("status"),
                    "%s did not reach the spawn on a genuine idle-timeout schedule (reason %r), "
                    "so the asymmetry recorded above is not the trigger check"
                    % (stem, result.get("reason")))
                self.assertIn(
                    "--idle-commit-worker-only", cmd or [],
                    "%s started something that is not the idle-commit worker" % stem)

    def test_only_one_hook_spends_the_session_source(self) -> None:
        """The agent copy requires an argument it never reads."""
        self.assertIn(
            "--query", _parser_options(AGENT),
            "the agent hook no longer defines --query, so not forwarding it is no longer an "
            "omission -- strike this part of the record")

        _result, agent_a = _spawn(AGENT, trigger_policy="idle_timeout", session_id_source="AAAA")
        _result, agent_b = _spawn(AGENT, trigger_policy="idle_timeout", session_id_source="BBBB")
        self.assertEqual(
            agent_a, agent_b,
            "the agent hook's child command now depends on session_id_source. That is the fix -- "
            "strike this part of the record")
        self.assertNotIn(
            "--query", agent_a,
            "the agent hook now forwards --query to the child")

        _result, codex_a = _spawn(CODEX, trigger_policy="idle_timeout", session_id_source="AAAA")
        _result, codex_b = _spawn(CODEX, trigger_policy="idle_timeout", session_id_source="BBBB")
        self.assertNotEqual(
            codex_a, codex_b,
            "the Codex hook's child command no longer depends on session_id_source either, so "
            "nothing here separates the two copies")
        self.assertIn("--query", codex_a, "the Codex hook no longer forwards --query")
        self.assertEqual(
            "idle commit worker for AAAA", codex_a[codex_a.index("--query") + 1],
            "the Codex hook forwards a --query built some other way")

    def test_a_failed_spawn_is_recorded_differently(self) -> None:
        """Same failure, two shapes of stored text."""
        broken = OSError("line one\nline two   with   spaces\nline three")
        agent_result, _cmd = _spawn(
            AGENT, trigger_policy="idle_timeout", session_id_source="sess-1", explode=broken)
        codex_result, _cmd = _spawn(
            CODEX, trigger_policy="idle_timeout", session_id_source="sess-1", explode=broken)

        for stem, result in ((AGENT, agent_result), (CODEX, codex_result)):
            with self.subTest(module=stem):
                self.assertEqual(
                    "error", result.get("status"),
                    "%s did not report the failed spawn as an error, so this fixture never "
                    "reached the branch under test" % stem)
                self.assertEqual("idle_commit_worker_spawn_failed", result.get("reason"))

        self.assertIn(
            "\n", agent_result.get("error", ""),
            "the agent hook now folds a multi-line spawn error. That is the fix -- strike this "
            "part of the record")
        self.assertNotIn(
            "\n", codex_result.get("error", ""),
            "the Codex hook no longer folds a multi-line spawn error")
        self.assertNotIn(
            "   ", codex_result.get("error", ""),
            "the Codex hook no longer collapses runs of spaces in a spawn error")

    def test_the_two_copies_read_the_same_environment(self) -> None:
        """The negative control, and the reason this is filed as its own drift.

        A separate finding says some hook controls reach the Codex hook and not the agent hook.
        This divergence is not an instance of it: the two copies have the SAME environment
        surface, and what differs is a missing statement. If one copy ever starts reading a
        control the other does not, this fails and says the cause has changed.
        """
        agent_reads, agent_writes = _environment_surface(AGENT)
        codex_reads, codex_writes = _environment_surface(CODEX)
        self.assertEqual(
            RECORDED_ENV_READS, agent_reads,
            "the agent hook's copy no longer reads exactly the recorded environment names")
        self.assertEqual(
            RECORDED_ENV_READS, codex_reads,
            "the Codex hook's copy no longer reads exactly the recorded environment names")
        self.assertEqual(
            agent_reads, codex_reads,
            "one copy now reads a control the other does not. This file records a missing "
            "statement, not a control that reaches one hook only -- if that is what this has "
            "become, it belongs with that finding instead")
        self.assertEqual(RECORDED_ENV_WRITES, agent_writes)
        self.assertEqual(RECORDED_ENV_WRITES, codex_writes)

    def test_the_command_value_helpers_are_not_the_divergence(self) -> None:
        """Two names for the same behaviour is enough to make a body hash differ. It is not drift.

        Asserted over the values that decide whether a flag is emitted at all, because that is
        what a difference here would change.
        """
        agent_helper = getattr(_import(AGENT), "append_cli_value", None)
        codex_helper = getattr(_import(CODEX), "_append_cli_value", None)
        self.assertIsNotNone(agent_helper, "the agent hook no longer defines append_cli_value")
        self.assertIsNotNone(codex_helper, "the Codex hook no longer defines _append_cli_value")
        for value in ("", "x", 0, None, False, "  ", "a b"):
            with self.subTest(value=value):
                left, right = [], []
                agent_helper(left, "--flag", value)
                codex_helper(right, "--flag", value)
                self.assertEqual(
                    left, right,
                    "the two command-value helpers now disagree about %r. That is a NEW "
                    "divergence and a bigger one than this file records: it changes every flag "
                    "the child is started with" % (value,))


if __name__ == "__main__":
    unittest.main()
