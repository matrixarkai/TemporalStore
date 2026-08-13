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

    def _write_emitted_codex_rows(self, work: Path) -> None:
        work.mkdir(parents=True, exist_ok=True)
        (work / ".emitted").write_text("", encoding="utf-8")
        (work / "backfill.codex.jsonl").write_text(
            '{"session_id":"s1","event":"UserPromptSubmit","ts_ms":1,"text":"fresh context one"}\n'
            '{"session_id":"s2","event":"UserPromptSubmit","ts_ms":2,"text":"fresh context two"}\n',
            encoding="utf-8",
        )

    def _run_daemon(self, temp_root: Path, work: Path, store: Path) -> subprocess.CompletedProcess[str]:
        env = dict(os.environ)
        env.update(
            MATRIXARK_BACKFILL_WORK=str(work),
            MATRIXARK_BACKFILL_AGENTS="codex",
            MATRIXARK_BACKFILL_CHUNK="1",
            MATRIXARK_BACKFILL_YIELD_MS="0",
            MATRIXARK_CODEX_RUST_HOOK_ROOT=str(store),
            CARGO_TARGET_DIR=str(temp_root / "target"),
            TEMPORALSTORE_PYTHON="python3",
            MATRIXARK_BACKFILL_REEMIT_ON_FRESH="0",
        )
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
            self.assertTrue((store / ".matrixark-local-context-backfill.complete.json").exists())
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
            self.assertTrue((store / ".matrixark-local-context-backfill.complete.json").exists())

    def test_completed_store_marker_skips_reingest(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            temp_root = Path(td)
            work = temp_root / "work"
            store = temp_root / "codex-rust"
            self._make_batch_stub(temp_root)
            self._write_emitted_codex_rows(work)
            (store / "indexes").mkdir(parents=True)
            (store / "indexes" / "shard-1.index.json").write_text('{"context_events":[1]}', encoding="utf-8")
            (store / ".matrixark-local-context-backfill.complete.json").write_text("{}", encoding="utf-8")

            proc = self._run_daemon(temp_root, work, store)

            self.assertEqual(proc.returncode, 0, proc.stderr)
            self.assertEqual("0", (work / ".offset.codex").read_text(encoding="utf-8").strip() if (work / ".offset.codex").exists() else "0")
            self.assertTrue((work / ".done").exists())


if __name__ == "__main__":
    unittest.main()
