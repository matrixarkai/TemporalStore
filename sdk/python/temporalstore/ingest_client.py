#!/usr/bin/env python3
"""Thin enterprise HTTP client for TemporalStore real-time ingestion + retrieval.

For verticals that own their LLM loop (no CLI hooks). Call the HTTP API — NOT MCP-stdio,
which is a per-process, single-client protocol for CLI agents. This client:

  * buffers intermediate ("provisional") messages per session and flushes on the final turn,
  * POSTs to /api/ingest (async fast-ack) in batches to cut round-trips,
  * retries on 429 / 5xx / network errors with exponential backoff,
  * uses a content-derived idempotency key so retries never double-ingest.

Dependency-light: standard library only (urllib). Thread-safe per client.

Streaming loop pattern:
    c = TemporalStoreClient("https://ts.example.com", api_key="...", account_id="acct", user_id="u1")
    c.add(sid, "user", user_text)                       # provisional -> buffered
    for step in agent_steps:
        c.add(sid, "tool", step.tool_result)            # provisional -> buffered (auto-flush at threshold)
    c.finalize(sid, "assistant", final_answer)          # final -> flush the whole turn, mark boundary
    ctx = c.retrieve(sid, query=next_user_msg)          # pull managed context for the next prompt
"""
from __future__ import annotations

import hashlib
import json
import threading
import time
import urllib.error
import urllib.request
from typing import Any, Optional

Json = dict[str, Any]


class TemporalStoreError(RuntimeError):
    pass


class TemporalStoreClient:
    def __init__(
        self,
        base_url: str,
        api_key: str,
        *,
        account_id: Optional[str] = None,
        tenant_id: Optional[str] = None,
        user_id: Optional[str] = None,
        timeout: float = 10.0,
        max_retries: int = 3,
        backoff_base_s: float = 0.25,
        flush_threshold: int = 16,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.scope_defaults = {
            k: v for k, v in (("account_id", account_id), ("tenant_id", tenant_id), ("user_id", user_id)) if v
        }
        self.timeout = timeout
        self.max_retries = max(0, max_retries)
        self.backoff_base_s = backoff_base_s
        self.flush_threshold = max(1, flush_threshold)
        self._buffers: dict[str, list[Json]] = {}
        self._lock = threading.Lock()

    # ---- scope ----------------------------------------------------------------
    def _scope(self, session_id: str) -> Json:
        return {**self.scope_defaults, "session_id": session_id}

    # ---- buffering ------------------------------------------------------------
    def add(self, session_id: str, role: str, content: str, *, finality: str = "provisional",
            metadata: Optional[Json] = None) -> Optional[Json]:
        """Buffer a message. Auto-flushes (provisional) once the buffer hits flush_threshold."""
        if not content:
            return None
        msg: Json = {"role": role, "content": content, "finality": finality}
        if metadata:
            msg["metadata"] = metadata
        with self._lock:
            buf = self._buffers.setdefault(session_id, [])
            buf.append(msg)
            over = len(buf) >= self.flush_threshold
        return self.flush(session_id) if over else None

    def finalize(self, session_id: str, role: str, content: str, *, metadata: Optional[Json] = None) -> Json:
        """Append the final assistant message and flush the whole turn as a session boundary."""
        self.add(session_id, role, content, finality="final", metadata=metadata)
        return self.flush(session_id, final=True)

    def flush(self, session_id: str, *, final: bool = False) -> Json:
        """Send the buffered batch to /api/ingest (async fast-ack). Idempotent per batch content."""
        with self._lock:
            batch = self._buffers.pop(session_id, [])
        if not batch:
            return {"status": "empty", "ingested": 0}
        body: Json = {
            "kind": "message",
            "scope": self._scope(session_id),
            "messages": batch,
        }
        if final:
            body["final_session_boundary"] = True
        idem = self._idempotency_key(session_id, batch, final)
        result = self._post("/api/ingest", body, idempotency_key=idem)
        result.setdefault("ingested", len(batch))
        return result

    # ---- retrieval ------------------------------------------------------------
    def retrieve(self, session_id: str, query: str, *, max_context_tokens: int = 8000,
                 context_source_mode: Optional[str] = None, local_context: Optional[list] = None) -> Json:
        body: Json = {
            "scope": self._scope(session_id),
            "query": query,
            "max_context_tokens": max_context_tokens,
        }
        if context_source_mode:
            body["context_source_mode"] = context_source_mode
        if local_context:
            body["local_context"] = local_context
        return self._post("/api/retrieve", body)

    def commit(self, session_id: str) -> Json:
        self.flush(session_id, final=True)
        return self._post("/api/session_commit", {"scope": self._scope(session_id), "final_session_boundary": True})

    # ---- transport (retry + idempotency) --------------------------------------
    @staticmethod
    def _idempotency_key(session_id: str, batch: list[Json], final: bool) -> str:
        payload = json.dumps([session_id, final, batch], sort_keys=True, separators=(",", ":"))
        return "ts-" + hashlib.sha256(payload.encode("utf-8")).hexdigest()[:32]

    def _post(self, path: str, body: Json, *, idempotency_key: Optional[str] = None) -> Json:
        if idempotency_key:
            body.setdefault("idempotency_key", idempotency_key)
        data = json.dumps(body).encode("utf-8")
        headers = {"Content-Type": "application/json", "Authorization": f"Bearer {self.api_key}"}
        if idempotency_key:
            headers["Idempotency-Key"] = idempotency_key
        url = self.base_url + path
        last_exc: Optional[Exception] = None
        for attempt in range(self.max_retries + 1):
            try:
                req = urllib.request.Request(url, data=data, headers=headers, method="POST")
                with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                    return json.loads(resp.read().decode("utf-8") or "{}")
            except urllib.error.HTTPError as exc:
                # Retry only on transient statuses; 4xx (except 429) is a client error -> raise.
                if exc.code in (429, 500, 502, 503, 504) and attempt < self.max_retries:
                    self._sleep_backoff(attempt, exc.headers.get("Retry-After"))
                    last_exc = exc
                    continue
                raise TemporalStoreError(f"{path} failed ({exc.code}): {exc.read()[:200]!r}") from exc
            except (urllib.error.URLError, TimeoutError) as exc:
                if attempt < self.max_retries:
                    self._sleep_backoff(attempt, None)
                    last_exc = exc
                    continue
                raise TemporalStoreError(f"{path} network error: {exc}") from exc
        raise TemporalStoreError(f"{path} exhausted retries: {last_exc}")

    def _sleep_backoff(self, attempt: int, retry_after: Optional[str]) -> None:
        if retry_after:
            try:
                time.sleep(min(30.0, float(retry_after)))
                return
            except (TypeError, ValueError):
                pass
        time.sleep(self.backoff_base_s * (2 ** attempt))
