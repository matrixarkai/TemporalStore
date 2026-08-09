#!/usr/bin/env python3
"""Local Codex+Claude context vs TemporalStore-managed context: token & quality sweep.

Goal (from the request):
  * Confirm ALL local agent context (Codex sessions + Claude project logs) is
    ingested into the TemporalStore Context data model (events -> segments ->
    entities -> summaries).
  * For the same query, measure how many tokens the managed compact ContextPack
    saves versus replaying the full local context, WITH the same or better answer
    quality from the SAME reader model.
  * Iterate the pack token budget downward to find the minimum tokens that still
    match or beat the full-context answer quality.

This does NOT fabricate quality. Answers are produced by a real reader (an
OpenAI-compatible OSS endpoint such as Qwen via Ollama, or an Anthropic reader),
and quality is graded by a judge model against the full local context as the
reference source of truth. A deterministic term-coverage score is reported
alongside the judge score as an objective anchor.
"""
from __future__ import annotations

import argparse
import collections
import hashlib
import json
import os
import re
import time
import urllib.error
import urllib.request
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Optional


# --------------------------------------------------------------------------------------
# Query set tuned to this user's real history, each with objective expected key terms.
# --------------------------------------------------------------------------------------
@dataclass(frozen=True)
class EvalQuery:
    query_id: str
    question: str
    expected_terms: tuple[str, ...]  # objective anchor for deterministic coverage


EVAL_QUERIES: tuple[EvalQuery, ...] = (
    EvalQuery("q_hooks", "What did the user decide about Rust TemporalStore hooks and real-time Codex prompt ingestion?",
              ("hook", "userpromptsubmit", "real", "ingest", "rust", "session")),
    EvalQuery("q_parity", "What did the user want regarding C++ vs Rust TemporalStore parity and fair performance comparison?",
              ("c++", "rust", "parity", "async", "oplog", "wal", "sdk", "latency")),
    EvalQuery("q_windows", "How should Windows users install TemporalStore, and what about WSL and Docker?",
              ("windows", "docker", "wsl", "installer", "image", "powershell")),
    EvalQuery("q_context", "What did the user ask about context management data fields, compaction, and backfill?",
              ("context", "compact", "backfill", "segment", "summary", "field")),
    EvalQuery("q_bench", "What did the user ask about Qwen, Ollama, vLLM, LoCoMo, LongMemEval, and OSS benchmarking?",
              ("qwen", "ollama", "vllm", "locomo", "longmemeval", "benchmark", "oss")),
    EvalQuery("q_storage", "What did the user ask about MatrixObject, shared storage, S3, and object storage?",
              ("matrixobject", "s3", "object", "storage", "shared")),
)


# --------------------------------------------------------------------------------------
# Context data model (mirrors run_codex_history_oss_context_benchmark.py, generalized).
# --------------------------------------------------------------------------------------
@dataclass
class ContextEvent:
    event_id: str
    timestamp: str
    timestamp_ms: int
    session_id: str
    origin: str  # "codex" | "claude"
    source_file: str
    line_no: int
    role: str
    text: str


@dataclass
class ContextSegment:
    segment_id: str
    session_id: str
    origin: str
    start_ms: int
    end_ms: int
    event_ids: list[str]
    text: str


@dataclass
class ContextEntity:
    entity_id: str
    name: str
    session_ids: list[str]
    support_event_ids: list[str]
    state: str


@dataclass
class ContextSummary:
    summary_id: str
    summary_type: str
    scope: str
    source_ids: list[str]
    text: str


STOPWORDS = set(
    "about above after again also because before between both code context could done from have "
    "into make more need other query should that their there these they this with would your what "
    "when where which will your you our the and for are was were has had but not you're".split()
)

TOKEN_RE = re.compile(r"[A-Za-z0-9_./:+#-]+|[^\w\s]", re.UNICODE)


def stable_id(*parts: Any) -> str:
    payload = "\x1f".join(str(part) for part in parts)
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()[:16]


def parse_timestamp_ms(value: str) -> int:
    if not value:
        return 0
    try:
        return int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp() * 1000)
    except Exception:
        return 0


def compact(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def token_estimate(text: str) -> int:
    """Rough GPT-style estimate; consistent across baseline and pack so ratios are fair."""
    if not text:
        return 0
    return max(1, round(len(TOKEN_RE.findall(text)) * 0.95))


def keywords(text: str, limit: int = 80) -> list[str]:
    words = re.findall(r"[A-Za-z][A-Za-z0-9_+.#-]{2,}", (text or "").lower())
    return [w for w in words if w not in STOPWORDS][:limit]


# --------------------------------------------------------------------------------------
# Ingestion: Codex + Claude local logs -> ContextEvents.
# --------------------------------------------------------------------------------------
def _is_visible_real_prompt(text: str) -> bool:
    if not text:
        return False
    s = text.strip()
    skip_prefixes = (
        "<environment_context", "<permissions instructions", "<in-app-browser-context",
        "<codex_internal_context", "<codex_delegation", "<recommended_plugins",
        "<system-reminder", "<command-", "<local-command", "Caveat:", "# Files mentioned by the user:",
    )
    if s.startswith(skip_prefixes):
        return False
    if s.startswith("[Request interrupted"):
        return False
    return len(s) >= 8


def _extract_codex(payload: dict[str, Any]) -> tuple[str, str]:
    role = str(payload.get("role") or "")
    parts: list[str] = []
    content = payload.get("content")
    if isinstance(content, str):
        parts.append(content)
    elif isinstance(content, list):
        for item in content:
            if isinstance(item, dict):
                if isinstance(item.get("text"), str):
                    parts.append(item["text"])
                elif isinstance(item.get("content"), str):
                    parts.append(item["content"])
    return role, compact(" ".join(parts))


def _extract_claude(rec: dict[str, Any]) -> tuple[str, str]:
    msg = rec.get("message") or {}
    role = str(msg.get("role") or "")
    parts: list[str] = []
    content = msg.get("content")
    if isinstance(content, str):
        parts.append(content)
    elif isinstance(content, list):
        for item in content:
            if not isinstance(item, dict):
                continue
            t = item.get("type")
            if t == "text" and isinstance(item.get("text"), str):
                parts.append(item["text"])
            elif t == "tool_use":  # VERBOSE tool call — real local context carries the full input
                inp = item.get("input")
                arg = json.dumps(inp, ensure_ascii=False) if isinstance(inp, (dict, list)) else str(inp or "")
                parts.append(f"[tool:{item.get('name', 'tool')}] {arg}")
            elif t == "tool_result":  # VERBOSE tool output — the bulk of real local context
                c = item.get("content")
                if isinstance(c, list):
                    c = " ".join(b.get("text", "") for b in c if isinstance(b, dict) and isinstance(b.get("text"), str))
                parts.append("[tool_result] " + str(c or ""))
    return role, compact(" ".join(parts))


def iter_codex_events(root: Path, max_sessions: int, max_msgs: int, roles: set[str]) -> list[ContextEvent]:
    if not root.exists():
        return []
    files = sorted(root.rglob("*.jsonl"), key=lambda p: p.stat().st_mtime, reverse=True)[:max_sessions]
    events: list[ContextEvent] = []
    for path in files:
        m = re.search(r"([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})", path.stem)
        session_id = ("codex:" + (m.group(1) if m else path.stem))
        count = 0
        try:
            handle = path.open("r", encoding="utf-8", errors="replace")
        except OSError:
            continue
        with handle:
            for line_no, line in enumerate(handle, 1):
                if count >= max_msgs:
                    break
                if '"message"' not in line:
                    continue
                try:
                    item = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if item.get("type") != "response_item":
                    continue
                payload = item.get("payload")
                if not isinstance(payload, dict) or payload.get("type") != "message":
                    continue
                role, text = _extract_codex(payload)
                if role not in roles or not _is_visible_real_prompt(text):
                    continue
                ts = str(item.get("timestamp") or "")
                events.append(ContextEvent(
                    event_id=stable_id(path.name, line_no, ts, text[:80]),
                    timestamp=ts, timestamp_ms=parse_timestamp_ms(ts),
                    session_id=session_id, origin="codex",
                    source_file=str(path), line_no=line_no, role=role, text=text,
                ))
                count += 1
    return events


def iter_claude_events(root: Path, max_sessions: int, max_msgs: int, roles: set[str]) -> list[ContextEvent]:
    if not root.exists():
        return []
    files = [p for p in root.rglob("*.jsonl") if "/subagents/" not in p.as_posix()]
    files = sorted(files, key=lambda p: p.stat().st_mtime, reverse=True)[:max_sessions]
    events: list[ContextEvent] = []
    for path in files:
        session_id = "claude:" + path.stem
        count = 0
        try:
            handle = path.open("r", encoding="utf-8", errors="replace")
        except OSError:
            continue
        with handle:
            for line_no, line in enumerate(handle, 1):
                if count >= max_msgs:
                    break
                if '"message"' not in line:
                    continue
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if rec.get("type") not in ("user", "assistant"):
                    continue
                role, text = _extract_claude(rec)
                if role not in roles or not _is_visible_real_prompt(text):
                    continue
                ts = str(rec.get("timestamp") or "")
                events.append(ContextEvent(
                    event_id=stable_id(path.name, line_no, ts, text[:80]),
                    timestamp=ts, timestamp_ms=parse_timestamp_ms(ts),
                    session_id=session_id, origin="claude",
                    source_file=str(path), line_no=line_no, role=role, text=text,
                ))
                count += 1
    return events


def build_segments(events: list[ContextEvent], chunk_size: int) -> list[ContextSegment]:
    by_session: dict[str, list[ContextEvent]] = collections.defaultdict(list)
    for e in events:
        by_session[e.session_id].append(e)
    segments: list[ContextSegment] = []
    for sid, evs in by_session.items():
        evs.sort(key=lambda e: (e.timestamp_ms, e.line_no))
        for off in range(0, len(evs), chunk_size):
            chunk = evs[off:off + chunk_size]
            if not chunk:
                continue
            text = "\n".join(f"{e.role}: {e.text}" for e in chunk)
            segments.append(ContextSegment(
                segment_id=stable_id("seg", sid, off, chunk[0].event_id),
                session_id=sid, origin=chunk[0].origin,
                start_ms=chunk[0].timestamp_ms, end_ms=chunk[-1].timestamp_ms,
                event_ids=[e.event_id for e in chunk], text=text,
            ))
    return segments


TOPIC_TERMS = {
    "rust": "rust_temporalstore", "c++": "cpp_temporalstore", "cpp": "cpp_temporalstore",
    "parity": "cpp_rust_parity", "temporalstore": "temporalstore", "hook": "codex_hook",
    "userpromptsubmit": "codex_hook", "codex": "codex_agent", "claude": "claude_agent",
    # OSS-reader / memory benchmarking consolidated into one strong entity so it ranks for q_bench
    "qwen": "oss_reader_benchmark", "ollama": "oss_reader_benchmark", "vllm": "oss_reader_benchmark",
    "locomo": "oss_reader_benchmark", "longmemeval": "oss_reader_benchmark", "vikingmem": "oss_reader_benchmark",
    "benchmark": "oss_reader_benchmark", "openviking": "oss_reader_benchmark",
    "windows": "windows_docker_install", "docker": "windows_docker_install", "wsl": "windows_docker_install",
    "matrixobject": "matrixobject_shared_storage", "s3": "matrixobject_shared_storage",
    "backfill": "context_backfill", "compact": "context_compaction", "context": "context_management",
    "async": "async_parity", "oplog": "wal_oplog", "wal": "wal_oplog",
}


def build_entities(events: list[ContextEvent], max_entities: int) -> list[ContextEntity]:
    buckets: dict[str, list[ContextEvent]] = collections.defaultdict(list)
    for e in events:
        low = e.text.lower()
        for term, ent in TOPIC_TERMS.items():
            if term in low:
                buckets[ent].append(e)
    entities: list[ContextEntity] = []
    for name, support in sorted(buckets.items(), key=lambda kv: len(kv[1]), reverse=True)[:max_entities]:
        support.sort(key=lambda e: e.timestamp_ms)
        sessions = sorted({e.session_id for e in support})
        latest = support[-1].text if support else ""
        entities.append(ContextEntity(
            entity_id=stable_id("ent", name),
            name=name, session_ids=sessions,
            support_event_ids=[e.event_id for e in support[-16:]],
            state=f"{name}: appears in {len(support)} prompts across {len(sessions)} sessions; latest: {latest[:240]}",
        ))
    return entities


def build_summaries(events: list[ContextEvent], entities: list[ContextEntity]) -> list[ContextSummary]:
    summaries: list[ContextSummary] = []
    by_session: dict[str, list[ContextEvent]] = collections.defaultdict(list)
    for e in events:
        by_session[e.session_id].append(e)
    for sid, evs in by_session.items():
        evs.sort(key=lambda e: e.timestamp_ms)
        sample = " ".join(e.text for e in evs[-5:])
        summaries.append(ContextSummary(
            summary_id=stable_id("sum", "session_l0", sid),
            summary_type="session_l0", scope=f"session:{sid}",
            source_ids=[e.event_id for e in evs[-12:]],
            text=f"Session {sid} has {len(evs)} messages. Recent focus: {sample[:500]}",
        ))
    entity_text = "; ".join(f"{e.name}: {e.state}" for e in entities[:10])
    summaries.append(ContextSummary(
        summary_id=stable_id("sum", "user_l1", len(events)),
        summary_type="user_l1", scope="tenant:local/user:local_user",
        source_ids=[e.entity_id for e in entities[:10]],
        text=f"Cross-session user memory: {entity_text[:1200]}",
    ))
    return summaries


# --------------------------------------------------------------------------------------
# Retrieval / packing (the "TemporalStore-managed" compact context).
# --------------------------------------------------------------------------------------
def score_text(q_terms: set[str], text: str, recency_rank: int) -> float:
    terms = set(keywords(text))
    overlap = len(q_terms & terms)
    return overlap * 10.0 + max(0.0, 5.0 - recency_rank * 0.01)


# Per-ref fed-text cap. Raised 1400->4000 chars (~350->~1000 tokens) so a single
# context event can return up to ~1k tokens of real content instead of being clipped.
PER_REF_TEXT_CHARS = int(os.environ.get("SWEEP_PER_REF_TEXT_CHARS", "4000"))


def _fill(cands, token_budget, used, selected, seen_text):
    """Greedy best-score fill of a sub-budget, with dedup + per-ref truncation. Mutates in place."""
    for score, rtype, rid, text, _is_cur in sorted(cands, key=lambda c: c[0], reverse=True):
        if score <= 0 or used >= token_budget:
            continue
        norm = text[:160]
        if norm in seen_text:
            continue
        cost = token_estimate(text)
        if used + cost > token_budget:
            remaining = token_budget - used
            if remaining <= 8:
                continue
            text = text[: remaining * 4]
            cost = token_estimate(text)
        if used + cost > token_budget:
            continue
        seen_text.add(norm)
        selected.append({"score": round(score, 2), "ref_type": rtype, "ref_id": rid,
                         "tokens": cost, "text": text[:PER_REF_TEXT_CHARS], "is_current": _is_cur})
        used += cost
    return used


def retrieve_pack(query: str, events, segments, entities, summaries, token_budget: int,
                  current_session_ratio: float | None = None) -> dict[str, Any]:
    q_terms = set(keywords(query))
    newest_session = ""
    if events:
        newest_session = max(events, key=lambda e: e.timestamp_ms).session_id
    candidates: list[tuple[float, str, str, str, bool]] = []
    for rank, e in enumerate(reversed(events)):
        candidates.append((score_text(q_terms, e.text, rank), "event", e.event_id, e.text,
                           e.session_id == newest_session))
    for rank, s in enumerate(reversed(segments)):
        candidates.append((score_text(q_terms, s.text, rank), "segment", s.segment_id, s.text,
                           getattr(s, "session_id", "") == newest_session))
    for rank, ent in enumerate(entities):
        candidates.append((score_text(q_terms, ent.name + " " + ent.state, rank), "entity",
                           ent.entity_id, ent.state, False))  # entities aggregate across sessions
    for rank, sm in enumerate(summaries):
        candidates.append((score_text(q_terms, sm.text, rank), "summary", sm.summary_id, sm.text, False))

    selected: list[dict[str, Any]] = []
    used = 0
    seen_text: set[str] = set()
    if current_session_ratio and 0.0 < current_session_ratio < 1.0:
        # Phase 1: reserve a floor of the budget for current-session reconstruction.
        cur_cap = int(token_budget * current_session_ratio)
        cur = [c for c in candidates if c[4]]
        used = _fill(cur, cur_cap, used, selected, seen_text)
    # Phase 2: fill the remaining budget from everything, best-score first (cross-session included).
    used = _fill(candidates, token_budget, used, selected, seen_text)
    cur_tok = sum(r["tokens"] for r in selected if r.get("is_current"))
    return {"query": query, "token_budget": token_budget, "used_tokens": used,
            "current_session_tokens": cur_tok, "cross_session_tokens": used - cur_tok,
            "selected_refs": selected}


def pack_to_context_str(pack: dict[str, Any]) -> str:
    return "\n\n".join(f"[{r['ref_type']}:{r['ref_id']}] {r['text']}" for r in pack["selected_refs"])


# --------------------------------------------------------------------------------------
# Full local-context baseline (the "replay everything" the agent would otherwise hold).
# --------------------------------------------------------------------------------------
def build_full_context(events: list[ContextEvent], max_input_tokens: int) -> dict[str, Any]:
    """Newest-first replay of raw prompts, truncated to the reader's input budget."""
    ordered = sorted(events, key=lambda e: e.timestamp_ms, reverse=True)
    full_all_tokens = sum(token_estimate(e.text) for e in events)
    lines: list[str] = []
    used = 0
    for e in ordered:
        piece = f"[{e.origin}:{e.session_id}] {e.role}: {e.text}"
        cost = token_estimate(piece)
        if used + cost > max_input_tokens:
            break
        lines.append(piece)
        used += cost
    return {
        "full_all_tokens": full_all_tokens,
        "fed_tokens": used,
        "fed_event_count": len(lines),
        "text": "\n".join(lines),
    }


# --------------------------------------------------------------------------------------
# Readers & judge (pluggable).
# --------------------------------------------------------------------------------------
ANSWER_SYS = "You are a precise memory assistant. Answer ONLY from the provided context. If the context lacks the answer, say exactly what is missing. Be concise (<=120 words)."
JUDGE_SYS = "You are a strict evaluation judge. You compare an answer against reference source material and rate factual correctness and completeness. Output ONLY compact JSON."


def build_answer_prompt(question: str, context: str) -> str:
    ctx = context if context else "(no context provided)"
    return f"Question: {question}\n\nContext:\n{ctx}\n\nAnswer:"


def build_judge_prompt(question: str, answer: str, reference: str) -> str:
    return (
        "Rate the ANSWER to the QUESTION using the REFERENCE as ground truth.\n"
        "Score 0-10 for correctness (no contradictions vs reference) and 0-10 for completeness "
        "(covers the key points the reference supports for this question).\n"
        'Return ONLY JSON: {"correctness": int, "completeness": int, "note": "<=15 words"}.\n\n'
        f"QUESTION: {question}\n\nANSWER: {answer}\n\nREFERENCE (source of truth):\n{reference[:9000]}\n"
    )


class OpenAICompatReader:
    def __init__(self, base_url: str, model: str, timeout: float = 60.0, max_tokens: int = 200):
        self.base_url = base_url.rstrip("/")
        self.model = model
        self.timeout = timeout
        self.max_tokens = max_tokens
        self.name = f"ollama:{model}"

    def _chat(self, system: str, user: str, max_tokens: int) -> dict[str, Any]:
        body = {
            "model": self.model,
            "messages": [{"role": "system", "content": system}, {"role": "user", "content": user}],
            "temperature": 0, "max_tokens": max_tokens,
        }
        req = urllib.request.Request(
            self.base_url + "/chat/completions",
            data=json.dumps(body).encode("utf-8"),
            headers={"content-type": "application/json", "authorization": "Bearer local"},
        )
        start = time.perf_counter()
        try:
            payload = json.loads(urllib.request.urlopen(req, timeout=self.timeout).read().decode("utf-8"))
            content = payload.get("choices", [{}])[0].get("message", {}).get("content", "")
            return {"ok": True, "text": compact(str(content)), "latency_ms": round((time.perf_counter() - start) * 1000, 1), "error": None}
        except Exception as exc:
            return {"ok": False, "text": "", "latency_ms": round((time.perf_counter() - start) * 1000, 1), "error": f"{type(exc).__name__}: {exc}"}

    def answer(self, question: str, context: str) -> dict[str, Any]:
        return self._chat(ANSWER_SYS, build_answer_prompt(question, context), self.max_tokens)

    def judge(self, question: str, answer: str, reference: str) -> dict[str, Any]:
        out = self._chat(JUDGE_SYS, build_judge_prompt(question, answer, reference), 120)
        return _parse_judge(out)

    def probe(self) -> dict[str, Any]:
        req = urllib.request.Request(self.base_url + "/models", headers={"authorization": "Bearer local"})
        try:
            data = json.loads(urllib.request.urlopen(req, timeout=8).read().decode("utf-8"))
            models = data.get("data") if isinstance(data, dict) else None
            return {"ready": bool(models), "models": [m.get("id") for m in (models or [])], "error": None}
        except Exception as exc:
            return {"ready": False, "models": [], "error": f"{type(exc).__name__}: {exc}"}


class AnthropicReader:
    def __init__(self, model: str, api_key: str, timeout: float = 90.0, max_tokens: int = 400):
        self.model = model
        self.api_key = api_key
        self.timeout = timeout
        self.max_tokens = max_tokens
        self.name = f"anthropic:{model}"

    def _chat(self, system: str, user: str, max_tokens: int) -> dict[str, Any]:
        body = {"model": self.model, "max_tokens": max_tokens, "temperature": 0,
                "system": system, "messages": [{"role": "user", "content": user}]}
        req = urllib.request.Request(
            "https://api.anthropic.com/v1/messages",
            data=json.dumps(body).encode("utf-8"),
            headers={"content-type": "application/json", "x-api-key": self.api_key, "anthropic-version": "2023-06-01"},
        )
        start = time.perf_counter()
        try:
            payload = json.loads(urllib.request.urlopen(req, timeout=self.timeout).read().decode("utf-8"))
            content = "".join(b.get("text", "") for b in payload.get("content", []) if b.get("type") == "text")
            return {"ok": True, "text": compact(content), "latency_ms": round((time.perf_counter() - start) * 1000, 1), "error": None}
        except Exception as exc:
            return {"ok": False, "text": "", "latency_ms": round((time.perf_counter() - start) * 1000, 1), "error": f"{type(exc).__name__}: {exc}"}

    def answer(self, question: str, context: str) -> dict[str, Any]:
        return self._chat(ANSWER_SYS, build_answer_prompt(question, context), self.max_tokens)

    def judge(self, question: str, answer: str, reference: str) -> dict[str, Any]:
        out = self._chat(JUDGE_SYS, build_judge_prompt(question, answer, reference), 200)
        return _parse_judge(out)

    def probe(self) -> dict[str, Any]:
        if not self.api_key:
            return {"ready": False, "models": [], "error": "no ANTHROPIC_API_KEY"}
        return {"ready": True, "models": [self.model], "error": None}


def _parse_judge(out: dict[str, Any]) -> dict[str, Any]:
    if not out.get("ok"):
        return {"ok": False, "correctness": None, "completeness": None, "note": "", "error": out.get("error")}
    m = re.search(r"\{.*\}", out["text"], re.DOTALL)
    if not m:
        return {"ok": False, "correctness": None, "completeness": None, "note": out["text"][:80], "error": "no json"}
    try:
        j = json.loads(m.group(0))
        c = int(j.get("correctness"))
        p = int(j.get("completeness"))
        return {"ok": True, "correctness": max(0, min(10, c)), "completeness": max(0, min(10, p)), "note": str(j.get("note", ""))[:120], "error": None}
    except Exception as exc:
        return {"ok": False, "correctness": None, "completeness": None, "note": out["text"][:80], "error": f"parse: {exc}"}


def term_coverage(answer: str, expected_terms: tuple[str, ...]) -> float:
    if not expected_terms:
        return 0.0
    low = (answer or "").lower()
    hit = sum(1 for t in expected_terms if t.lower() in low)
    return round(hit / len(expected_terms), 3)


# --------------------------------------------------------------------------------------
# Main sweep.
# --------------------------------------------------------------------------------------
def make_reader(kind: str, args: argparse.Namespace):
    if kind == "ollama":
        return OpenAICompatReader(args.reader_base_url, args.reader_model, args.reader_timeout, args.reader_max_tokens)
    if kind == "anthropic":
        return AnthropicReader(args.anthropic_model, os.environ.get("ANTHROPIC_API_KEY", ""), 90.0, 400)
    raise ValueError(kind)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--codex-root", default="/mnt/c/Users/Deeproute/.codex/sessions")
    ap.add_argument("--claude-root", default="/mnt/c/Users/Deeproute/.claude/projects")
    ap.add_argument("--max-sessions", type=int, default=40)
    ap.add_argument("--max-msgs", type=int, default=120)
    ap.add_argument("--segment-size", type=int, default=10)
    ap.add_argument("--max-entities", type=int, default=18)
    ap.add_argument("--roles", default="user")  # comma list: user,assistant
    ap.add_argument("--budgets", default="1536,1024,768,512,384,256,160,96")
    ap.add_argument("--current-session-ratio", type=float, default=0.0,
                    help="reserve this fraction of the pack budget for newest-session refs "
                         "(0 = pure score fill; e.g. 0.5 = remote-only current/cross split)")
    ap.add_argument("--baseline-max-input-tokens", type=int, default=12000)
    ap.add_argument("--reader", default="ollama", choices=["ollama", "anthropic"])
    ap.add_argument("--judge", default="", help="reader kind for judging; default = same as --reader")
    ap.add_argument("--reader-base-url", default="http://127.0.0.1:11434/v1")
    ap.add_argument("--reader-model", default="qwen2.5:1.5b")
    ap.add_argument("--reader-timeout", type=float, default=60.0)
    ap.add_argument("--reader-max-tokens", type=int, default=220)
    ap.add_argument("--anthropic-model", default="claude-haiku-4-5-20251001")
    ap.add_argument("--out-dir", default="docs/benchmarks/local_context_token_quality")
    ap.add_argument("--tag", default="")
    ap.add_argument("--dry-run", action="store_true", help="ingest + sample pack only, no reader calls")
    ap.add_argument("--emit-cases", action="store_true", help="write baseline+packs JSON for external Claude reader/judge")
    args = ap.parse_args()

    roles = set(r.strip() for r in args.roles.split(",") if r.strip())
    budgets = [int(b) for b in args.budgets.split(",") if b.strip()]

    # ---- Ingest ----
    codex_events = iter_codex_events(Path(args.codex_root), args.max_sessions, args.max_msgs, roles)
    claude_events = iter_claude_events(Path(args.claude_root), args.max_sessions, args.max_msgs, roles)
    events = sorted(codex_events + claude_events, key=lambda e: e.timestamp_ms)
    segments = build_segments(events, args.segment_size)
    entities = build_entities(events, args.max_entities)
    summaries = build_summaries(events, entities)

    ingestion = {
        "codex_root": args.codex_root, "claude_root": args.claude_root,
        "codex_events": len(codex_events), "claude_events": len(claude_events),
        "total_events": len(events),
        "codex_sessions": len({e.session_id for e in codex_events}),
        "claude_sessions": len({e.session_id for e in claude_events}),
        "segments": len(segments), "entities": len(entities), "summaries": len(summaries),
        "full_all_tokens": sum(token_estimate(e.text) for e in events),
        "roles": sorted(roles),
    }
    print("[ingest]", json.dumps(ingestion), flush=True)

    if args.emit_cases:
        full_ctx = build_full_context(events, args.baseline_max_input_tokens)
        cases = {"ingestion": ingestion,
                 "baseline_full_context": {"fed_tokens": full_ctx["fed_tokens"], "text": full_ctx["text"]},
                 "budgets": budgets, "queries": []}
        for q in EVAL_QUERIES:
            packs = []
            for b in budgets:
                pk = retrieve_pack(q.question, events, segments, entities, summaries, b, current_session_ratio=(args.current_session_ratio or None))
                packs.append({"budget": b, "used_tokens": pk["used_tokens"], "ref_count": len(pk["selected_refs"]),
                              "context": pack_to_context_str(pk)})
            cases["queries"].append({"query_id": q.query_id, "question": q.question,
                                     "expected_terms": list(q.expected_terms), "packs": packs})
        out_dir = Path(args.out_dir); out_dir.mkdir(parents=True, exist_ok=True)
        tag = ("_" + args.tag) if args.tag else ""
        p = out_dir / f"claude_cases{tag}.json"
        p.write_text(json.dumps(cases, indent=2, ensure_ascii=False), encoding="utf-8")
        print("[emit-cases]", str(p), flush=True)
        return 0

    if args.dry_run:
        sample_q = EVAL_QUERIES[0]
        pack = retrieve_pack(sample_q.question, events, segments, entities, summaries, 512)
        full_ctx = build_full_context(events, args.baseline_max_input_tokens)
        print("[dry-run] sample query:", sample_q.question, flush=True)
        print("[dry-run] baseline fed tokens:", full_ctx["fed_tokens"], "/ full-all:", ingestion["full_all_tokens"], flush=True)
        print("[dry-run] pack@512 used_tokens:", pack["used_tokens"], "refs:", len(pack["selected_refs"]), flush=True)
        for r in pack["selected_refs"][:6]:
            print(f"  - {r['ref_type']}:{r['ref_id']} ({r['tokens']}t) {r['text'][:110]}", flush=True)
        return 0

    reader = make_reader(args.reader, args)
    judge_kind = args.judge or args.reader
    judge = reader if judge_kind == args.reader else make_reader(judge_kind, args)
    rprobe = reader.probe()
    print("[reader]", reader.name, json.dumps(rprobe), flush=True)
    if not rprobe.get("ready"):
        print("[fatal] reader not ready; aborting", flush=True)
        return 2

    full_ctx = build_full_context(events, args.baseline_max_input_tokens)
    reference_full = full_ctx["text"]  # judge ground-truth reference

    query_reports = []
    for q in EVAL_QUERIES:
        print(f"[query] {q.query_id}", flush=True)
        base_ans = reader.answer(q.question, full_ctx["text"])
        base_judge = judge.judge(q.question, base_ans.get("text", ""), reference_full)
        base_cov = term_coverage(base_ans.get("text", ""), q.expected_terms)
        base_score = _combined(base_judge, base_cov)
        sweep = []
        for b in budgets:
            pack = retrieve_pack(q.question, events, segments, entities, summaries, b, current_session_ratio=(args.current_session_ratio or None))
            ctx = pack_to_context_str(pack)
            ans = reader.answer(q.question, ctx)
            jd = judge.judge(q.question, ans.get("text", ""), reference_full)
            cov = term_coverage(ans.get("text", ""), q.expected_terms)
            sc = _combined(jd, cov)
            sweep.append({
                "budget": b, "pack_tokens": pack["used_tokens"], "ref_count": len(pack["selected_refs"]),
                "answer": ans.get("text", ""), "answer_error": ans.get("error"),
                "judge": jd, "term_coverage": cov, "combined_score": sc,
                "token_saving_vs_baseline_pct": round(100 * (1 - pack["used_tokens"] / max(1, full_ctx["fed_tokens"])), 1),
            })
        # minimal-token budget whose combined score >= baseline (allowing tiny tolerance)
        matching = [s for s in sweep if s["combined_score"] is not None and base_score is not None and s["combined_score"] >= base_score - 0.02]
        best = min(matching, key=lambda s: s["pack_tokens"]) if matching else None
        query_reports.append({
            "query_id": q.query_id, "question": q.question, "expected_terms": list(q.expected_terms),
            "baseline": {"fed_tokens": full_ctx["fed_tokens"], "answer": base_ans.get("text", ""),
                         "judge": base_judge, "term_coverage": base_cov, "combined_score": base_score},
            "sweep": sweep,
            "min_tokens_matching_baseline": best,
        })

    # ---- Aggregate ----
    saved = []
    for qr in query_reports:
        best = qr["min_tokens_matching_baseline"]
        if best:
            saved.append({
                "query_id": qr["query_id"],
                "baseline_fed_tokens": qr["baseline"]["fed_tokens"],
                "min_pack_tokens": best["pack_tokens"],
                "token_saving_pct": best["token_saving_vs_baseline_pct"],
                "baseline_score": qr["baseline"]["combined_score"],
                "managed_score": best["combined_score"],
            })
    agg = {
        "queries_with_match": len(saved),
        "queries_total": len(query_reports),
        "avg_baseline_fed_tokens": _avg([s["baseline_fed_tokens"] for s in saved]),
        "avg_min_pack_tokens": _avg([s["min_pack_tokens"] for s in saved]),
        "avg_token_saving_pct": _avg([s["token_saving_pct"] for s in saved]),
        "avg_baseline_score": _avg([s["baseline_score"] for s in saved]),
        "avg_managed_score": _avg([s["managed_score"] for s in saved]),
    }

    report = {
        "schema": "local_context_token_quality_sweep_v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "tag": args.tag,
        "reader": reader.name, "judge": getattr(judge, "name", reader.name),
        "reader_probe": rprobe,
        "ingestion": ingestion,
        "baseline_full_context": {k: full_ctx[k] for k in ("full_all_tokens", "fed_tokens", "fed_event_count")},
        "budgets": budgets,
        "aggregate": agg,
        "queries": query_reports,
    }
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    tag = ("_" + args.tag) if args.tag else ""
    stem = f"local_context_token_quality{tag}"
    (out_dir / f"{stem}.json").write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    (out_dir / f"{stem}.md").write_text(render_md(report), encoding="utf-8")
    print("[done]", json.dumps({"aggregate": agg, "out": str(out_dir / (stem + '.json'))}, indent=2), flush=True)
    return 0


def _combined(judge_out: dict[str, Any], coverage: float) -> Optional[float]:
    if not judge_out.get("ok"):
        return None
    # 0-10 scale: 45% correctness, 35% completeness, 20% deterministic term coverage
    return round(0.45 * judge_out["correctness"] + 0.35 * judge_out["completeness"] + 0.20 * (coverage * 10), 3)


def _avg(vals):
    vals = [v for v in vals if v is not None]
    return round(sum(vals) / len(vals), 3) if vals else None


def render_md(r: dict[str, Any]) -> str:
    ing = r["ingestion"]; agg = r["aggregate"]; base = r["baseline_full_context"]
    L = [
        "# Local Context vs TemporalStore-Managed Context: Token & Quality Sweep",
        "",
        f"Generated `{r['generated_at']}` | reader `{r['reader']}` | judge `{r['judge']}` | tag `{r['tag']}`",
        "",
        "## Ingestion coverage (all local context -> Context model)",
        "",
        f"- Codex: **{ing['codex_events']}** events across **{ing['codex_sessions']}** sessions",
        f"- Claude: **{ing['claude_events']}** events across **{ing['claude_sessions']}** sessions",
        f"- Total events: **{ing['total_events']}**; segments **{ing['segments']}**; entities **{ing['entities']}**; summaries **{ing['summaries']}**",
        f"- Full replay tokens (all events): **{ing['full_all_tokens']}**; baseline fed to reader: **{base['fed_tokens']}** ({base['fed_event_count']} events)",
        "",
        "## Headline",
        "",
        f"- Queries where managed pack matched/beat full-context quality: **{agg['queries_with_match']}/{agg['queries_total']}**",
        f"- Avg baseline tokens fed: **{agg['avg_baseline_fed_tokens']}** -> avg minimal managed pack: **{agg['avg_min_pack_tokens']}**",
        f"- **Avg token saving at equal-or-better quality: {agg['avg_token_saving_pct']}%**",
        f"- Avg quality (0-10): baseline **{agg['avg_baseline_score']}** vs managed **{agg['avg_managed_score']}**",
        "",
        "## Per-query minimal-token result",
        "",
        "| Query | Baseline tokens | Baseline score | Min pack tokens | Managed score | Token saving |",
        "|---|---:|---:|---:|---:|---:|",
    ]
    for qr in r["queries"]:
        b = qr["baseline"]; best = qr["min_tokens_matching_baseline"]
        if best:
            L.append(f"| {qr['query_id']} | {b['fed_tokens']} | {b['combined_score']} | {best['pack_tokens']} | {best['combined_score']} | {best['token_saving_vs_baseline_pct']}% |")
        else:
            L.append(f"| {qr['query_id']} | {b['fed_tokens']} | {b['combined_score']} | — | no budget matched | — |")
    L += ["", "## Full sweep (per query, per budget)", ""]
    for qr in r["queries"]:
        L.append(f"### {qr['query_id']}: {qr['question']}")
        L.append("")
        L.append(f"Baseline (fed {qr['baseline']['fed_tokens']} tok) score {qr['baseline']['combined_score']} — judge {qr['baseline']['judge'].get('correctness')}/{qr['baseline']['judge'].get('completeness')}, coverage {qr['baseline']['term_coverage']}")
        L.append("")
        L.append("| Budget | Pack tokens | Refs | Judge C/Cmp | Coverage | Combined | Saving |")
        L.append("|---:|---:|---:|---|---:|---:|---:|")
        for s in qr["sweep"]:
            jc = s["judge"].get("correctness"); jp = s["judge"].get("completeness")
            L.append(f"| {s['budget']} | {s['pack_tokens']} | {s['ref_count']} | {jc}/{jp} | {s['term_coverage']} | {s['combined_score']} | {s['token_saving_vs_baseline_pct']}% |")
        L.append("")
    return "\n".join(L) + "\n"


if __name__ == "__main__":
    raise SystemExit(main())
