#!/usr/bin/env python3
"""Backfill TemporalStore from *local agent context* on disk (Claude + Codex).

Unlike ``matrixark_context_backfill.py`` (which replays a MatrixKV raw record log
into serving records), this tool warms the store from the real local-context
surfaces the agents leave on disk so that retrieval is not cold on the next turn:

  * Claude Code transcripts   ~/.claude/projects/<proj>/<session>.jsonl
  * Codex session rollouts     ~/.codex/sessions/YYYY/MM/DD/rollout-*.jsonl
  * Codex dual-hook captures    /tmp/matrixark-codex-dual-hook*.json
  * Markdown / resource files   CLAUDE.md, AGENTS.md, MEMORY.md, docs/*.md, ...

Every item is normalized into a Claude/Codex *hook payload* and fed through the
exact same engine the live hooks use -- the built ``codex_context_hook`` rust
binary -- so ingested records are byte-identical to live ingestion (same
``extract_context`` path, same ``agent=...; role=...: text`` body, same session
index). No new storage path, no parallel writer.

Design notes
------------
* Role is derived from the hook *event* (UserPromptSubmit->user, Stop->assistant,
  PostToolUse->tool), exactly as the engine's ``role_for_event`` does, so we set
  ``--event`` per item rather than trusting the source's own role label.
* Per-agent identity/scope/store-root mirror the live hooks so backfilled nodes
  land in the same store the live hook retrieves from:
      claude -> acct_claude/tenant_claude, root /tmp/temporalstore-rust-claude-hook
      codex  -> acct_codex /tenant_codex , root /tmp/temporalstore-rust-codex-hook
* Idempotent: the engine keys each node on hash(text), so re-runs converge.
* Resources are ingested under a shared ``_resources`` session into *both* agent
  stores (opt out with --no-shared-resources). Surfacing shared/global nodes in
  arbitrary live sessions is a follow-up (teach the hook to also load the
  ``_resources`` node set, or use the python promotion pipeline) -- this tool
  makes the data present and correct; it does not change the live retrieval scope.
"""
from __future__ import annotations

import argparse
import glob
import json
import os
import subprocess
import sys
import time
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Iterator

DEFAULT_BIN = "/tmp/temporalstore-context-workflow-target/debug/codex_context_hook"

# Per-agent identity mirrors the live hooks (see matrixark_claude_hook.sh and
# codex_context_hook.rs defaults). user_id defaults to the process user like live.
AGENTS = {
    "claude": dict(account="acct_claude", tenant="tenant_claude",
                   root="/tmp/temporalstore-rust-claude-hook"),
    "codex": dict(account="acct_codex", tenant="tenant_codex",
                  root="/tmp/temporalstore-rust-codex-hook"),
}

# Map a normalized role to the hook event that yields that role in the engine.
EVENT_FOR_ROLE = {"user": "UserPromptSubmit", "assistant": "Stop", "tool": "PostToolUse"}

# Stable cross-session scope: durable memory/resources/skills ingested here are
# retrieved by every session (the hook does a second retrieval over this scope).
GLOBAL_SESSION = "_global"

# System/wrapper blocks in Codex rollouts that are harness scaffolding, not memory.
_SYSTEM_PREFIXES = ("<environment_context", "<permissions", "<user_instructions",
                    "<permissions instructions", "<system", "<available_tools")


def _homes() -> tuple[str, str]:
    """Return (windows_user_home_on_wsl, tmp_dir), auto-detecting the WSL mount.

    Discovers the Windows user profile mounted under WSL (e.g. /mnt/<drive>/Users/
    <name>) without hardcoding any user; override with MATRIXARK_WIN_HOME. Falls
    back to the POSIX home when not running under WSL.
    """
    candidates: list[str] = []
    env_home = os.environ.get("MATRIXARK_WIN_HOME")
    if env_home:
        candidates.append(env_home)
    mount_root = os.environ.get("MATRIXARK_WSL_MOUNT", "/mnt")
    for drive in ("c", "d", "e"):
        users_dir = os.path.join(mount_root, drive, "Users")
        if os.path.isdir(users_dir):
            for name in sorted(os.listdir(users_dir)):
                if name in ("Public", "Default", "Default User", "All Users"):
                    continue
                home = os.path.join(users_dir, name)
                if os.path.isdir(os.path.join(home, ".codex")) or os.path.isdir(os.path.join(home, ".claude")):
                    candidates.append(home)
    candidates.append(os.path.expanduser("~"))
    for cand in candidates:
        if cand and os.path.isdir(cand):
            return cand, "/tmp"
    return os.path.expanduser("~"), "/tmp"


@dataclass
class Item:
    agent: str
    session_id: str
    event: str
    payload: dict
    source: str  # provenance tag for metrics
    text: str = ""      # extracted text (for --emit-jsonl / batch bin)
    ts_ms: int = 0      # source event time in ms (0 -> engine uses now)


@dataclass
class Metrics:
    scanned: int = 0
    fed: int = 0
    accepted: int = 0
    skipped: int = 0
    failed: int = 0
    per_source: dict = field(default_factory=dict)

    def bump(self, source: str, key: str) -> None:
        d = self.per_source.setdefault(source, {"fed": 0, "accepted": 0, "skipped": 0, "failed": 0})
        d[key] = d.get(key, 0) + 1


# ----------------------------- text extraction ------------------------------

def _content_to_text(content) -> str:
    if isinstance(content, str):
        return content.strip()
    if isinstance(content, list):
        parts = []
        for block in content:
            if isinstance(block, str):
                parts.append(block)
            elif isinstance(block, dict):
                # claude: {type:text,text}; codex: {type:input_text/output_text,text}
                if isinstance(block.get("text"), str):
                    parts.append(block["text"])
        return "\n".join(p for p in parts if p).strip()
    return ""


def _is_system_noise(text: str) -> bool:
    t = text.lstrip()
    return t.startswith(_SYSTEM_PREFIXES)


def _is_low_value(text: str, max_chars: int = 20000) -> tuple[bool, str]:
    """Drop non-memory noise: base64/data-URI blobs, binary dumps. Returns
    (drop, cleaned_text). Long-but-textual content is truncated, not dropped."""
    if not text:
        return True, ""
    head = text[:300]
    if "data:image" in head or "base64," in head or "data:application/octet-stream" in head:
        return True, ""
    # base64/binary blob heuristic: long text with almost no whitespace
    if len(text) > 1500:
        ws = sum(c.isspace() for c in text[:4000])
        if ws / min(len(text), 4000) < 0.02:
            return True, ""
    if len(text) > max_chars:
        text = text[:max_chars] + "\n...[truncated]"
    return False, text


def _iso_to_ms(value) -> int:
    """Best-effort ISO-8601 -> epoch ms; 0 on failure (engine falls back to now)."""
    if not isinstance(value, str) or not value:
        return 0
    v = value.strip().replace("Z", "+00:00")
    try:
        from datetime import datetime
        return int(datetime.fromisoformat(v).timestamp() * 1000)
    except Exception:
        return 0


# ------------------------------- source readers -----------------------------

def iter_claude_transcripts(home: str, limit: int | None) -> Iterator[Item]:
    root = os.path.join(home, ".claude", "projects")
    # top-level session files only: <proj>/<uuid>.jsonl (skip subagents/tool-results dirs)
    files = sorted(glob.glob(os.path.join(root, "*", "*.jsonl")))
    for fp in files:
        sid = Path(fp).stem
        n = 0
        for line in _read_lines(fp):
            d = _loads(line)
            if not d:
                continue
            msg = d.get("message") or {}
            role = msg.get("role")
            if role not in ("user", "assistant"):
                continue
            text = _content_to_text(msg.get("content"))
            if not text or _is_system_noise(text):
                continue
            ev = EVENT_FOR_ROLE[role]
            yield Item("claude", sid, ev,
                       {"hook_event_name": ev, "session_id": sid, "prompt": text},
                       "claude_transcript", text=text, ts_ms=_iso_to_ms(d.get("timestamp")))
            n += 1
            if limit and n >= limit:
                break


def iter_codex_rollouts(home: str, limit: int | None,
                        min_assistant_chars: int = 400, user_only: bool = False) -> Iterator[Item]:
    """Codex rollouts contain one message per reasoning/tool micro-step, so the
    raw stream is ~200k items dominated by short intermediate assistant turns.
    For store-warming we keep every *user* turn (the queries -- high signal) and
    only *substantive* assistant turns (>= min_assistant_chars), dropping the
    micro-step noise. Set user_only to ingest queries alone."""
    root = os.path.join(home, ".codex", "sessions")
    files = sorted(glob.glob(os.path.join(root, "**", "rollout-*.jsonl"), recursive=True))
    for fp in files:
        sid = Path(fp).stem
        n = 0
        for line in _read_lines(fp):
            d = _loads(line)
            if not d or d.get("type") != "response_item":
                if d and d.get("type") == "session_meta":
                    sid = ((d.get("payload") or {}).get("id")) or sid
                continue
            pay = d.get("payload") or {}
            if pay.get("type") != "message":
                continue
            role = pay.get("role")
            if role not in ("user", "assistant"):
                continue
            text = _content_to_text(pay.get("content"))
            if not text or _is_system_noise(text):
                continue
            if role == "assistant" and (user_only or len(text) < min_assistant_chars):
                continue
            ev = EVENT_FOR_ROLE[role]
            yield Item("codex", sid, ev,
                       {"hook_event_name": ev, "session_id": sid, "prompt": text},
                       "codex_rollout", text=text, ts_ms=_iso_to_ms(d.get("timestamp")))
            n += 1
            if limit and n >= limit:
                break


def iter_codex_dual_hooks(tmp: str, limit: int | None) -> Iterator[Item]:
    files = sorted(glob.glob(os.path.join(tmp, "matrixark-codex-dual-hook*.json")))
    n = 0
    for fp in files:
        d = _loads(_read_all(fp))
        if not d or not isinstance(d, dict):
            continue
        event = d.get("hook_event_name") or "UserPromptSubmit"
        sid = d.get("session_id") or d.get("thread_id") or Path(fp).stem
        # best-effort text for the emit/batch path (mirrors engine payload_text order)
        text = ""
        for k in ("prompt", "last_assistant_message", "assistant_message", "response", "output"):
            if isinstance(d.get(k), str) and d[k].strip():
                text = d[k].strip(); break
        if not text and isinstance(d.get("tool_response"), (str, dict, list)):
            text = json.dumps(d["tool_response"])[:4000] if not isinstance(d["tool_response"], str) else d["tool_response"][:4000]
        # feed the captured hook payload as-is (--bin path): highest fidelity to live ingestion
        yield Item("codex", str(sid), event, d, "codex_dual_hook",
                   text=text, ts_ms=_iso_to_ms(d.get("timestamp")))
        n += 1
        if limit and n >= limit:
            break


def iter_openviking(home: str, limit: int | None) -> Iterator[Item]:
    """OpenViking/VikingMem local memory state as a backfill source.

    OpenViking is the local memory engine referenced by the context system
    (docs/context_openviking_parity_cases.md); its on-disk state under
    ~/.openviking holds session transcripts and cli config. We ingest its
    transcript/memory jsonl lines as codex-scope memory so single-node recall
    includes what OpenViking already captured locally."""
    roots = [os.path.join(home, ".openviking"),
             os.path.join(home, ".codex", "plugins", "openviking-local")]
    files: list[str] = []
    for r in roots:
        files += glob.glob(os.path.join(r, "**", "*.jsonl"), recursive=True)
        files += glob.glob(os.path.join(r, "**", "*.conf"), recursive=True)
    n = 0
    for fp in sorted(set(files)):
        sid = GLOBAL_SESSION  # openviking memory -> cross-session global scope
        if fp.endswith(".conf"):
            body = _read_all(fp).strip()
            if body:
                yield Item("codex", sid, "PostToolUse",
                           {"hook_event_name": "PostToolUse", "session_id": sid, "prompt": body},
                           "openviking", text=body, ts_ms=0)
                n += 1
            continue
        for line in _read_lines(fp):
            d = _loads(line)
            if not d:
                continue
            # transcript-ish: role/content or text/message fields
            role = (d.get("role") or (d.get("message") or {}).get("role") or "user")
            text = _content_to_text(d.get("content") or (d.get("message") or {}).get("content")) \
                or (d.get("text") if isinstance(d.get("text"), str) else "") \
                or (d.get("prompt") if isinstance(d.get("prompt"), str) else "")
            text = (text or "").strip()
            if not text or _is_system_noise(text):
                continue
            ev = EVENT_FOR_ROLE.get(role, "UserPromptSubmit")
            yield Item("codex", sid, ev,
                       {"hook_event_name": ev, "session_id": sid, "prompt": text},
                       "openviking", text=text, ts_ms=_iso_to_ms(d.get("timestamp")))
            n += 1
            if limit and n >= limit:
                return


def iter_resources(globs: list[str], max_chars: int, limit: int | None) -> Iterator[tuple[str, str, int]]:
    """Yield (relpath, chunk_text, chunk_index) for every resource chunk."""
    seen: set[str] = set()
    files: list[str] = []
    for g in globs:
        for fp in glob.glob(g, recursive=True):
            if os.path.isfile(fp) and fp not in seen:
                seen.add(fp)
                files.append(fp)
    files.sort()
    count = 0
    for fp in files:
        text = _read_all(fp)
        if not text or not text.strip():
            continue
        for i, chunk in enumerate(_chunk(text, max_chars)):
            yield fp, chunk, i
            count += 1
            if limit and count >= limit:
                return


def _chunk(text: str, max_chars: int) -> Iterator[str]:
    buf: list[str] = []
    size = 0
    for line in text.splitlines(keepends=True):
        if size + len(line) > max_chars and buf:
            yield "".join(buf)
            buf, size = [], 0
        buf.append(line)
        size += len(line)
    if buf:
        yield "".join(buf)


# ------------------------------- io helpers ---------------------------------

def _read_lines(fp: str) -> Iterator[str]:
    try:
        with open(fp, encoding="utf-8", errors="ignore") as fh:
            for line in fh:
                line = line.strip()
                if line:
                    yield line
    except OSError:
        return


def _read_all(fp: str) -> str:
    try:
        with open(fp, encoding="utf-8", errors="ignore") as fh:
            return fh.read()
    except OSError:
        return ""


def _loads(s: str):
    try:
        return json.loads(s)
    except Exception:
        return None


# ------------------------------- feeding ------------------------------------

def feed(item: Item, binp: str, users: dict, max_ctx: int, timeout: int) -> str:
    cfg = AGENTS[item.agent]
    env = dict(os.environ)
    env.update(
        MATRIXARK_AGENT_NAME=item.agent,
        TEMPORALSTORE_AGENT_NAME=item.agent,
        MATRIXARK_ACCOUNT_ID=cfg["account"],
        MATRIXARK_TENANT_ID=cfg["tenant"],
        MATRIXARK_USER_ID=users[item.agent],
        TEMPORALSTORE_RUST_CODEX_HOOK_ROOT=cfg["root"],
        MATRIXARK_SESSION_ID=item.session_id,
        MATRIXARK_HOOK_MAX_CONTEXT_TOKENS=str(max_ctx),
        # keep the debug event log quiet during bulk backfill
        TEMPORALSTORE_RUST_CODEX_EVENT_LOG_ENABLE="0",
    )
    proc = subprocess.run(
        [binp, "--agent-name", item.agent, "--event", item.event, "--session-id", item.session_id],
        input=json.dumps(item.payload), text=True, capture_output=True, env=env, timeout=timeout,
    )
    return proc.stdout.strip()


def classify(stdout: str) -> str:
    d = _loads(stdout) or {}
    if d.get("status") == "skipped":
        return "skipped"
    st = ((d.get("ingest") or {}).get("status"))
    if st == "accepted":
        return "accepted"
    if st in (None, "") and d.get("status") == "ok":
        return "accepted"  # e.g. resource with empty retrieve but stored
    return "failed"


# --------------------------------- main -------------------------------------

def build_items(args, home, tmp) -> list[Item]:
    items: list[Item] = []
    per = args.limit_per_source
    if "transcripts" in args.sources and "claude" in args.agents:
        items += list(iter_claude_transcripts(home, per))
    if "rollouts" in args.sources and "codex" in args.agents:
        items += list(iter_codex_rollouts(home, per, args.rollout_min_assistant_chars, args.rollout_user_only))
    if "dual_hooks" in args.sources and "codex" in args.agents:
        items += list(iter_codex_dual_hooks(tmp, per))
    if "openviking" in args.sources:
        items += list(iter_openviking(home, per))
    if "resources" in args.sources:
        res_agents = list(args.agents) if args.shared_resources else args.agents[:1]
        for relpath, chunk, idx in iter_resources(args.resource_globs, args.max_chars, per):
            rel = os.path.relpath(relpath, home) if relpath.startswith(home) else relpath
            body = f"[resource {rel} #{idx}]\n{chunk}"
            mt = int(os.path.getmtime(relpath) * 1000) if os.path.exists(relpath) else 0
            for ag in res_agents:
                # durable knowledge -> stable "_global" scope so it surfaces cross-session
                items.append(Item(ag, GLOBAL_SESSION, "PostToolUse",
                                  {"hook_event_name": "PostToolUse", "session_id": GLOBAL_SESSION, "prompt": body},
                                  "resource_md", text=body, ts_ms=mt))
    return items


def main() -> int:
    home, tmp = _homes()
    # allow --home override before building default resource globs (parse it early)
    import argparse as _ap
    _pre = _ap.ArgumentParser(add_help=False)
    _pre.add_argument("--home", default=None)
    _known, _ = _pre.parse_known_args()
    if _known.home:
        home = _known.home
    # Curated allowlist (NO recursive ** over the .claude tree or the 800k-file
    # repo -- that firehose pulls thousands of stray/backup .md files and is slow
    # on the /mnt/c 9p mount). These are the real, durable resource surfaces.
    repo = os.environ.get("MATRIXARK_REPO", "/opt/github-services/TemporalStore")
    default_resources = [
        os.path.join(home, ".claude", "projects", "*", "memory", "MEMORY.md"),
        os.path.join(home, ".claude", "projects", "*", "memory", "*.md"),
        os.path.join(home, ".claude", "CLAUDE.md"),
        os.path.join(home, ".claude", "plugins", "**", "SKILL.md"),   # claude skills (if installed locally)
        os.path.join(home, ".codex", "AGENTS.md"),
        os.path.join(home, ".codex", "CLAUDE.md"),
        os.path.join(home, ".codex", "plugins", "**", "*.md"),        # codex skill/plugin md (bounded tree)
        os.path.join(repo, "README.md"),
        os.path.join(repo, "AGENTS.md"),
        os.path.join(repo, "CLAUDE.md"),
        os.path.join(repo, "docs", "*.md"),
    ]
    ap = argparse.ArgumentParser(description="Backfill TemporalStore from local Claude/Codex context on disk.")
    ap.add_argument("--agents", default="claude,codex", help="comma list: claude,codex")
    ap.add_argument("--sources", default="transcripts,rollouts,dual_hooks,openviking,resources",
                    help="comma list: transcripts,rollouts,dual_hooks,openviking,resources")
    ap.add_argument("--bin", default=os.environ.get("MATRIXARK_HOOK_BIN", DEFAULT_BIN),
                    help="per-event hook binary (slow O(n^2) path; prefer --emit-jsonl + batch bin)")
    ap.add_argument("--emit-jsonl", default=None, metavar="DIR",
                    help="fast enumerator mode: write per-agent {DIR}/backfill.<agent>.jsonl "
                         "({session_id,event,ts_ms,text}) for the load-once batch bin instead of "
                         "feeding the per-event binary")
    ap.add_argument("--run-batch", default=None, metavar="BATCH_BIN",
                    help="with --emit-jsonl, immediately run this batch bin once per agent over the "
                         "emitted files (load-once ingestion)")
    ap.add_argument("--resource-globs", default=",".join(default_resources),
                    help="comma-separated globs for md/resource files (recursive ** supported)")
    ap.add_argument("--home", default=None,
                    help="override the user-home root for sources (e.g. a fast ext4 stage dir "
                         "holding .claude/.codex/.openviking copies instead of the slow /mnt/c mount)")
    ap.add_argument("--claude-user", default=os.environ.get("USER", "root"))
    ap.add_argument("--codex-user", default=os.environ.get("USER", "root"))
    ap.add_argument("--rollout-min-assistant-chars", type=int, default=400,
                    help="drop codex-rollout assistant micro-steps shorter than this")
    ap.add_argument("--rollout-user-only", action="store_true",
                    help="ingest only user turns from codex rollouts (queries only)")
    ap.add_argument("--max-chars", type=int, default=1500, help="resource chunk size (chars)")
    ap.add_argument("--max-ctx", type=int, default=256, help="MATRIXARK_HOOK_MAX_CONTEXT_TOKENS during backfill")
    ap.add_argument("--limit-per-source", type=int, default=None, help="cap items per source (smoke tests)")
    ap.add_argument("--timeout", type=int, default=120)
    ap.add_argument("--dry-run", action="store_true", help="enumerate + normalize only, no ingestion")
    ap.add_argument("--no-shared-resources", dest="shared_resources", action="store_false",
                    help="ingest resources into only the first agent's store")
    ap.add_argument("--report", default=None, help="write JSON metrics to this path")
    ap.add_argument("--progress-every", type=int, default=50)
    args = ap.parse_args()
    args.agents = [a.strip() for a in args.agents.split(",") if a.strip() in AGENTS]
    args.sources = [s.strip() for s in args.sources.split(",") if s.strip()]
    args.resource_globs = [g.strip() for g in args.resource_globs.split(",") if g.strip()]
    users = {"claude": args.claude_user, "codex": args.codex_user}

    if not args.dry_run and not args.emit_jsonl and not os.path.exists(args.bin):
        print(f"ERROR: engine binary not found: {args.bin}\n"
              f"Build it (or run the Claude SessionStart hook) first, or pass --bin.", file=sys.stderr)
        return 2
    if args.run_batch and not os.path.exists(args.run_batch):
        print(f"ERROR: batch bin not found: {args.run_batch}", file=sys.stderr)
        return 2

    t0 = time.time()
    items = build_items(args, home, tmp)
    m = Metrics(scanned=len(items))

    # dry-run summary
    from collections import Counter
    by_src = Counter(it.source for it in items)
    by_agent = Counter(it.agent for it in items)
    print(f"home={home}  bin={args.bin}")
    print(f"enumerated {len(items)} items  by_source={dict(by_src)}  by_agent={dict(by_agent)}")
    if args.dry_run:
        # show a couple of samples
        for it in items[:3]:
            print(f"  sample [{it.source}] agent={it.agent} sid={it.session_id} event={it.event} "
                  f"text={(it.payload.get('prompt') or json.dumps(it.payload))[:80]!r}")
        if args.report:
            Path(args.report).write_text(json.dumps({"dry_run": True, "scanned": len(items),
                                                     "by_source": dict(by_src), "by_agent": dict(by_agent)}, indent=2))
        return 0

    # --- fast enumerator mode: emit per-agent JSONL for the load-once batch bin ---
    if args.emit_jsonl:
        outdir = Path(args.emit_jsonl)
        outdir.mkdir(parents=True, exist_ok=True)
        counts: dict[str, int] = {}
        dropped = 0
        handles = {}
        try:
            for it in items:
                drop, text = _is_low_value(it.text or "")
                if drop or not text.strip():
                    dropped += 1
                    continue
                fh = handles.get(it.agent)
                if fh is None:
                    fh = open(outdir / f"backfill.{it.agent}.jsonl", "w", encoding="utf-8")
                    handles[it.agent] = fh
                fh.write(json.dumps({"session_id": it.session_id, "event": it.event,
                                     "ts_ms": it.ts_ms, "text": text}, ensure_ascii=False) + "\n")
                counts[it.agent] = counts.get(it.agent, 0) + 1
        finally:
            for fh in handles.values():
                fh.close()
        print(f"emitted {sum(counts.values())} records -> {dict(counts)} (dropped {dropped} low-value) in {outdir}")
        if args.run_batch:
            for agent, n in counts.items():
                cfg = AGENTS[agent]
                env = dict(os.environ)
                env.update(MATRIXARK_AGENT_NAME=agent, TEMPORALSTORE_AGENT_NAME=agent,
                           MATRIXARK_ACCOUNT_ID=cfg["account"], MATRIXARK_TENANT_ID=cfg["tenant"],
                           MATRIXARK_USER_ID=users[agent], TEMPORALSTORE_RUST_CODEX_HOOK_ROOT=cfg["root"])
                infile = outdir / f"backfill.{agent}.jsonl"
                print(f"\n=== batch-ingest {agent}: {n} records -> {cfg['root']} ===", flush=True)
                with open(infile, encoding="utf-8") as fh:
                    proc = subprocess.run([args.run_batch, "--agent-name", agent],
                                          stdin=fh, text=True, capture_output=True, env=env)
                print(proc.stdout.strip())
                if proc.returncode != 0:
                    print(f"  batch bin rc={proc.returncode} stderr(tail)={proc.stderr.strip()[-400:]}")
        if args.report:
            Path(args.report).write_text(json.dumps({"emit": True, "counts": counts}, indent=2))
        return 0

    for i, it in enumerate(items, 1):
        try:
            out = feed(it, args.bin, users, args.max_ctx, args.timeout)
            cls = classify(out)
        except subprocess.TimeoutExpired:
            cls = "failed"
        m.fed += 1
        setattr(m, cls, getattr(m, cls) + 1)
        m.bump(it.source, cls if cls != "fed" else "fed")
        m.bump(it.source, "fed")
        if args.progress_every and i % args.progress_every == 0:
            dt = time.time() - t0
            print(f"  {i}/{len(items)}  accepted={m.accepted} skipped={m.skipped} failed={m.failed} "
                  f"({i/dt:.1f}/s)", flush=True)

    dt = time.time() - t0
    print(f"\nDONE in {dt:.1f}s: fed={m.fed} accepted={m.accepted} skipped={m.skipped} failed={m.failed}")
    for src, d in sorted(m.per_source.items()):
        print(f"  {src:18} {d}")
    if args.report:
        Path(args.report).write_text(json.dumps(asdict(m) | {"seconds": dt}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
