# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
import os
import subprocess
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
DAEMON = REPO_ROOT / "tools" / "matrixark_backfill_daemon.sh"


class BackfillDaemonFreshStartTest(unittest.TestCase):
    def _make_batch_stub(self, root: Path) -> None:
        batch = root / "target" / "debug" / "context_batch_ingest"
        batch.parent.mkdir(parents=True)
        batch.write_text(
            """#!/usr/bin/env bash
set -euo pipefail
count=0
while IFS= read -r _line; do
  count=$((count + 1))
done
mkdir -p "$TEMPORALSTORE_RUST_CODEX_HOOK_ROOT/indexes"
printf '{"context_events":[]}' > "$TEMPORALSTORE_RUST_CODEX_HOOK_ROOT/indexes/shard-1.index.json"
printf '{"accepted":%s,"failed":0}\\n' "$count"
""",
            encoding="utf-8",
        )
        batch.chmod(0o755)

    def _write_emitted_codex_rows(self, work: Path, count: int = 2) -> None:
        work.mkdir(parents=True, exist_ok=True)
        (work / ".emitted").write_text("", encoding="utf-8")
        (work / "backfill.codex.jsonl").write_text(
            "".join(
                f'{{"session_id":"s{i}","event":"UserPromptSubmit","ts_ms":{i},"text":"fresh context {i}"}}\n'
                for i in range(1, count + 1)
            ),
            encoding="utf-8",
        )

    def _run_daemon(
        self,
        temp_root: Path,
        work: Path,
        store: Path,
        *,
        chunk: str | None = "1",
    ) -> subprocess.CompletedProcess[str]:
        env = dict(os.environ)
        env.update(
            MATRIXARK_BACKFILL_WORK=str(work),
            MATRIXARK_BACKFILL_AGENTS="codex",
            MATRIXARK_BACKFILL_YIELD_MS="0",
            MATRIXARK_CODEX_RUST_HOOK_ROOT=str(store),
            CARGO_TARGET_DIR=str(temp_root / "target"),
            TEMPORALSTORE_PYTHON="python3",
            MATRIXARK_BACKFILL_REEMIT_ON_FRESH="0",
        )
        if chunk is not None:
            env["MATRIXARK_BACKFILL_CHUNK"] = chunk
        return subprocess.run(["bash", str(DAEMON)], cwd=REPO_ROOT, env=env, text=True, capture_output=True, timeout=30)

    def test_stale_done_marker_does_not_skip_empty_rust_store(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            temp_root = Path(td)
            work = temp_root / "work"
            store = temp_root / "codex-rust"
            self._make_batch_stub(temp_root)
            self._write_emitted_codex_rows(work)
            (work / ".done").write_text("", encoding="utf-8")

            proc = self._run_daemon(temp_root, work, store)

            self.assertEqual(proc.returncode, 0, proc.stderr)
            self.assertTrue((store / ".matrixark-local-context-backfill.codex.complete.json").exists())
            self.assertTrue((work / ".done").exists())

    def test_existing_live_prompt_record_without_marker_still_backfills(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            temp_root = Path(td)
            work = temp_root / "work"
            store = temp_root / "codex-rust"
            self._make_batch_stub(temp_root)
            self._write_emitted_codex_rows(work)
            (store / "indexes").mkdir(parents=True)
            (store / "indexes" / "shard-1.index.json").write_text('{"context_events":[1]}', encoding="utf-8")

            proc = self._run_daemon(temp_root, work, store)

            self.assertEqual(proc.returncode, 0, proc.stderr)
            self.assertTrue((store / ".matrixark-local-context-backfill.codex.complete.json").exists())

    def test_completed_store_marker_skips_reingest(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            temp_root = Path(td)
            work = temp_root / "work"
            store = temp_root / "codex-rust"
            self._make_batch_stub(temp_root)
            self._write_emitted_codex_rows(work)
            (store / "indexes").mkdir(parents=True)
            (store / "indexes" / "shard-1.index.json").write_text('{"context_events":[1]}', encoding="utf-8")
            (store / ".matrixark-local-context-backfill.codex.complete.json").write_text("{}", encoding="utf-8")

            proc = self._run_daemon(temp_root, work, store)

            self.assertEqual(proc.returncode, 0, proc.stderr)
            self.assertEqual("0", (work / ".offset.codex").read_text(encoding="utf-8").strip() if (work / ".offset.codex").exists() else "0")
            self.assertTrue((work / ".done").exists())

    def test_default_chunk_is_large_enough_to_reduce_process_churn(self) -> None:
        script = DAEMON.read_text(encoding="utf-8")
        self.assertIn('CHUNK="${MATRIXARK_BACKFILL_CHUNK:-4000}"', script)
        self.assertIn('MATRIXARK_BACKFILL_RAW_FIRST="${MATRIXARK_BACKFILL_RAW_FIRST:-1}"', script)
        self.assertIn('MATRIXARK_BACKFILL_CARGO_OFFLINE', script)
        self.assertIn('MATRIXARK_BACKFILL_BATCH_TIMEOUT_SECONDS', script)
        self.assertIn('--report "$EMIT_REPORT"', script)
        self.assertIn("CARGO_ARGS=(build --release -q -p temporalstore-rust --bin context_batch_ingest)", script)
        self.assertIn("unique_roots=0", script)
        self.assertIn("JOBS > unique_roots", script)

    def test_daemon_writes_status_for_completed_backfill(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            temp_root = Path(td)
            work = temp_root / "work"
            store = temp_root / "codex-rust"
            self._make_batch_stub(temp_root)
            self._write_emitted_codex_rows(work)

            proc = self._run_daemon(temp_root, work, store)

            self.assertEqual(proc.returncode, 0, proc.stderr)
            status = (work / "status.json").read_text(encoding="utf-8")
            self.assertIn('"phase":"completed"', status)
            self.assertIn('"elapsed_ms"', status)


if __name__ == "__main__":
    unittest.main()
