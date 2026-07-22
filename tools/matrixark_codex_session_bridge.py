#!/usr/bin/env python3
"""Bridge Codex desktop session user messages into the MatrixArk dual hook.

Codex CLI hooks fire for CLI turns, but some desktop/app-server turns are only
visible in the Codex session JSONL stream. This bridge treats that stream as the
authoritative Codex transcript and submits new user messages to the same
C++/Rust dual hook used by UserPromptSubmit.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any


Json = dict[str, Any]


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _default_sessions_root() -> Path:
    home = os.environ.get("USERPROFILE")
    if home:
        candidate = Path(home) / ".codex" / "sessions"
        if candidate.exists():
            return candidate
    return Path("/mnt/c/Users/Deeproute/.codex/sessions")


def _load_state(path: Path) -> Json:
    try:
        loaded = json.loads(path.read_text(encoding="utf-8"))
        return loaded if isinstance(loaded, dict) else {}
    except Exception:
        return {}


def _save_state(path: Path, state: Json) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(state, indent=2, sort_keys=True), encoding="utf-8")
    tmp.replace(path)


def _line_payload(line: str) -> Json | None:
    try:
        parsed = json.loads(line)
    except Exception:
        return None
    return parsed if isinstance(parsed, dict) else None


def _extract_session_meta(record: Json, fallback_path: Path) -> Json:
    if record.get("type") == "session_meta" and isinstance(record.get("payload"), dict):
        payload = record["payload"]
        return {
            "session_id": payload.get("session_id") or payload.get("id") or fallback_path.stem,
            "thread_id": payload.get("id") or payload.get("session_id") or fallback_path.stem,
            "workspace_root": payload.get("cwd") or "",
            "source": payload.get("originator") or payload.get("source") or "Codex",
        }
    return {}


def _is_user_authored_text(text: str) -> bool:
    stripped = text.strip()
    if not stripped:
        return False
    lowered = stripped.lower()
    if lowered.startswith(("<environment_context>", "<codex_internal_context>", "<developer_context>", "<system_context>")):
        return False
    return True


def _extract_user_message(record: Json) -> str:
    if record.get("type") == "event_msg" and isinstance(record.get("payload"), dict):
        payload = record["payload"]
        if payload.get("type") == "user_message":
            message = payload.get("message")
            text = message.strip() if isinstance(message, str) else ""
            return text if _is_user_authored_text(text) else ""
    return ""


def _session_files(root: Path, lookback_days: int) -> list[Path]:
    cutoff = time.time() - max(1, lookback_days) * 86400
    search_roots: list[Path] = []
    today = datetime.now()
    if (root / f"{today.year:04d}").exists():
        for offset in range(max(1, lookback_days) + 1):
            day = today - timedelta(days=offset)
            search_roots.append(root / f"{day.year:04d}" / f"{day.month:02d}" / f"{day.day:02d}")
    else:
        search_roots.append(root)
    files: list[Path] = []
    for search_root in search_roots:
        if not search_root.exists():
            continue
        try:
            candidates = search_root.glob("*.jsonl")
            for path in candidates:
                if path.is_file() and path.stat().st_mtime >= cutoff:
                    files.append(path)
        except OSError:
            continue
    return sorted(files, key=lambda path: path.stat().st_mtime, reverse=True)


def _event_key(path: Path, line_no: int, text: str) -> str:
    return f"{path}:{line_no}:{hash(text)}"


def _hook_payload(meta: Json, text: str, path: Path, line_no: int) -> Json:
    session_id = str(meta.get("session_id") or path.stem)
    thread_id = str(meta.get("thread_id") or session_id)
    messages = [{"role": "user", "content": text}]
    source_ref = f"{path}#{line_no}"
    return {
        "prompt": text,
        "user_prompt": text,
        "input": text,
        "message": text,
        "text": text,
        "role": "user",
        "messages": messages,
        "items": messages,
        "hookEventName": "UserPromptSubmit",
        "event": "UserPromptSubmit",
        "session_id": session_id,
        "thread_id": thread_id,
        "conversation_id": thread_id,
        "codex_thread_id": thread_id,
        "workspace_root": meta.get("workspace_root") or "",
        "source": "codex_session_bridge",
        "source_kind": "codex_session_jsonl",
        "source_ref": source_ref,
        "metadata": {
            "source": "codex_session_bridge",
            "source_kind": "codex_session_jsonl",
            "source_ref": source_ref,
        },
        "synthetic": False,
    }


def _run_dual_hook(repo: Path, payload: Json, timeout: int) -> bool:
    hook = repo / "tools" / "matrixark_codex_dual_hook.sh"
    env = os.environ.copy()
    env.setdefault("MATRIXARK_REPO_ROOT", str(repo))
    env.setdefault("MATRIXARK_HOOK_FAIL_OPEN", "1")
    completed = subprocess.run(
        ["bash", str(hook), "UserPromptSubmit"],
        cwd=str(repo),
        input=json.dumps(payload, separators=(",", ":")),
        text=True,
        capture_output=True,
        timeout=timeout,
        env=env,
    )
    if completed.returncode != 0:
        sys.stderr.write(completed.stderr[-4000:])
        return False
    return True


def scan_once(args: argparse.Namespace, state: Json) -> tuple[int, int]:
    repo = _repo_root()
    sessions_root = Path(args.sessions_root)
    seen = set(state.get("seen") or [])
    emitted = 0
    scanned = 0
    for path in _session_files(sessions_root, args.lookback_days):
        meta: Json = {"session_id": path.stem, "thread_id": path.stem}
        try:
            lines = path.read_text(encoding="utf-8").splitlines()
        except Exception:
            continue
        for line_no, line in enumerate(lines, start=1):
            record = _line_payload(line)
            if not record:
                continue
            meta.update({k: v for k, v in _extract_session_meta(record, path).items() if v})
            text = _extract_user_message(record)
            if not text:
                continue
            scanned += 1
            key = _event_key(path, line_no, text)
            if key in seen:
                continue
            payload = _hook_payload(meta, text, path, line_no)
            if args.dry_run:
                print(json.dumps(payload, ensure_ascii=False))
                seen.add(key)
                emitted += 1
                continue
            if _run_dual_hook(repo, payload, args.hook_timeout_sec):
                seen.add(key)
                emitted += 1
                if args.limit and emitted >= args.limit:
                    state["seen"] = sorted(seen)[-args.max_seen :]
                    return scanned, emitted
    state["seen"] = sorted(seen)[-args.max_seen :]
    return scanned, emitted


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--sessions-root", default=str(_default_sessions_root()))
    parser.add_argument("--state-file", default=str(_repo_root() / ".local/runtime/matrixark-codex-session-bridge/state.json"))
    parser.add_argument("--lookback-days", type=int, default=2)
    parser.add_argument("--interval-sec", type=float, default=2.0)
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--max-seen", type=int, default=20000)
    parser.add_argument("--hook-timeout-sec", type=int, default=120)
    parser.add_argument("--watch", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    state_path = Path(args.state_file)
    state = _load_state(state_path)
    while True:
        scanned, emitted = scan_once(args, state)
        _save_state(state_path, state)
        print(json.dumps({"status": "ok", "scanned_user_messages": scanned, "emitted": emitted, "watch": args.watch}))
        if not args.watch:
            return 0
        time.sleep(max(0.5, args.interval_sec))


if __name__ == "__main__":
    raise SystemExit(main())
