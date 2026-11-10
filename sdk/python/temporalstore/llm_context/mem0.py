#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Mem0-compatible SDK backed only by TemporalStore's LLM-context API.

Drop-in for ``from mem0 import Memory``. Point an existing client at a
TemporalStore proxy with a one-line import swap::

    # from mem0 import Memory
    from temporalstore.llm_context import Memory

    m = Memory()                         # env-configured, same as the ingest client
    m.add(messages, user_id="u1", agent_id="assistant", run_id="sess-42")
    m.search("what did we decide?", user_id="u1", agent_id="assistant", limit=8)

mem0 passes memory identity as TOP-LEVEL kwargs (not nested under a scope). This
shim maps those kwargs onto TemporalStore's canonical ``scope`` object:

    ============  ==========================
    mem0 kwarg    TemporalStore scope field
    ============  ==========================
    ``user_id``   ``scope.user_id``
    ``agent_id``  ``scope.agent_id``
    ``run_id``    ``scope.session_id``
    ============  ==========================

Only the Python standard library is required. ``base_url`` and ``api_key`` prefer
the ``TEMPORALSTORE_*`` environment names.
"""
from __future__ import annotations

# mem0 documents a ceiling of 1000 memories per batch call; refuse past it rather than
# quietly issuing an unbounded number of requests.
MEM0_BATCH_LIMIT = 1000

import json
import os
from typing import Any, Optional
from urllib.error import HTTPError
from urllib.parse import quote
from urllib.request import Request, urlopen

Json = dict[str, Any]

__all__ = ["Memory"]


def _resolve_base_url(base_url: Optional[str]) -> str:
    return (base_url or os.environ.get("TEMPORALSTORE_BASE_URL")
            or os.environ.get("TEMPORALSTORE_GATEWAY_URL")
            or "http://127.0.0.1:17000").rstrip("/")


def _resolve_api_key(api_key: Optional[str]) -> Optional[str]:
    return api_key or os.environ.get("TEMPORALSTORE_API_KEY") or None


def _request_json(base_url: str, api_key: Optional[str], method: str, path: str,
                  body: Optional[Json], timeout: float) -> Json:
    data = json.dumps(body, separators=(",", ":")).encode("utf-8") if body is not None else None
    headers = {"Accept": "application/json"}
    if data is not None:
        headers["Content-Type"] = "application/json"
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    request = Request(base_url + path, data=data, headers=headers, method=method)
    try:
        with urlopen(request, timeout=timeout) as response:
            raw = response.read()
    except HTTPError as error:
        raw = error.read()
        if error.code != 404:
            raise RuntimeError(f"{method} {path} failed: HTTP {error.code}: {raw[:256]!r}") from error
    return json.loads(raw) if raw else {}


def _post_json(base_url: str, api_key: Optional[str], path: str, body: Json,
               timeout: float) -> Json:
    return _request_json(base_url, api_key, "POST", path, body, timeout)


def _get_json(base_url: str, api_key: Optional[str], path: str, timeout: float) -> Json:
    return _request_json(base_url, api_key, "GET", path, None, timeout)


def _put_json(base_url: str, api_key: Optional[str], path: str, body: Json,
              timeout: float) -> Json:
    return _request_json(base_url, api_key, "PUT", path, body, timeout)


def _delete_json(base_url: str, api_key: Optional[str], path: str, timeout: float) -> Json:
    return _request_json(base_url, api_key, "DELETE", path, None, timeout)


def _provider_messages(value: Any) -> list[Json]:
    """Normalize common OpenAI/Anthropic response objects without another package."""
    if hasattr(value, "model_dump"):
        value = value.model_dump()
    if isinstance(value, dict) and isinstance(value.get("choices"), list):
        return _provider_messages([choice.get("message", {}) for choice in value["choices"]])
    if isinstance(value, dict) and isinstance(value.get("content"), list):
        text = "".join(str(part.get("text", "")) for part in value["content"]
                       if isinstance(part, dict) and part.get("type") == "text")
        return [{"role": str(value.get("role") or "assistant"), "content": text}]
    if isinstance(value, list):
        result: list[Json] = []
        for item in value:
            if hasattr(item, "model_dump"):
                item = item.model_dump()
            if isinstance(item, dict):
                content = item.get("content", item.get("text", ""))
                if isinstance(content, list):
                    content = "".join(str(part.get("text", "")) for part in content
                                      if isinstance(part, dict))
                result.append({"role": str(item.get("role") or "user"),
                               "content": str(content or "")})
        return result
    if isinstance(value, dict):
        return [{"role": str(value.get("role") or "user"),
                 "content": str(value.get("content", value.get("text", "")) or "")}]
    return [{"role": "user", "content": str(value)}]


def _reshape_search_results(res: Json) -> Json:
    """Map a TemporalStore context response to mem0's ``search`` shape.

    Native memory responses already use the target shape. Older grouped context
    responses are flattened while preserving their event/entity kind in metadata.
    """
    if isinstance(res.get("results"), list):
        return res
    refs = res.get("selected_refs")
    if not isinstance(refs, list):
        result = res.get("result")
        refs = result.get("selected_refs") if isinstance(result, dict) else None
    if not isinstance(refs, list):
        groups = res.get("groups")
        if not isinstance(groups, list):
            result = res.get("result")
            groups = result.get("groups") if isinstance(result, dict) else None
        if isinstance(groups, list):
            flattened: list[Json] = []
            for group in groups:
                if not isinstance(group, dict):
                    continue
                group_type = group.get("type")
                for item in group.get("items") or []:
                    if not isinstance(item, dict):
                        continue
                    if group_type and "ref_type" not in item:
                        item = {**item, "ref_type": group_type}
                    flattened.append(item)
            refs = flattened
    results: list[Json] = []
    for index, ref in enumerate(refs or []):
        if not isinstance(ref, dict):
            continue
        memory = ref.get("text") or ref.get("text_preview") or ref.get("citation") or ""
        stable_id = (
            ref.get("id")
            or ref.get("ref_id")
            or ref.get("source_ref")
            or ref.get("ref_hash")
            or ref.get("event_id_hash")
        )
        if stable_id in (None, ""):
            stable_id = f"ref-{index}-{abs(hash(memory)) & 0xFFFFFFFF:08x}"
        metadata = {
            key: value
            for key, value in ref.items()
            if key not in {"text", "text_preview", "citation", "score", "id", "ref_id"}
            and value not in (None, "", [], {})
        }
        results.append({
            "id": stable_id,
            "memory": memory,
            "score": ref.get("score"),
            "metadata": metadata,
        })
    return {"results": results}


def _add_memory_text(messages: Any) -> str:
    """Best-effort human text for a mem0 ``add`` results entry: the last user turn's content, else
    the last turn's content, else empty."""
    if not isinstance(messages, list):
        return ""
    last = ""
    for message in messages:
        if not isinstance(message, dict):
            continue
        content = message.get("content")
        if not isinstance(content, str) or not content:
            continue
        last = content
        if str(message.get("role") or "").lower() == "user":
            last = content
    return last


def _with_mem0_add_results(response: Json, messages: Any) -> Json:
    """Augment a TemporalStore add response with mem0's ``add`` return shape.

    Real mem0 ``add`` returns ``{"results": [{"id", "memory", "event"}]}`` and strict callers read
    ``result["results"][0]["id"]``. We keep every existing top-level field (``event_id_hash``,
    ``status``, ...) for backward compatibility AND add a single anchor ``results`` entry so both
    ``result["event_id_hash"]`` and ``result["results"][0]["id"]`` work.

    The ``event`` maps our keyed-upsert outcome: ``"ADD"`` (new / normal ingest), ``"UPDATE"`` (a
    keyed-upsert that superseded a prior record), ``"NONE"`` (a keyed-upsert rejected by the rank
    guard -- ``id`` is then the surviving existing record)."""
    if not isinstance(response, dict):
        return response
    # The gateway wraps the ingest result under {"accepted","scope","result": {...}}; a direct call
    # returns the flat ingest dict. Read the id/outcome from whichever carries event_id_hash.
    inner = response.get("result")
    payload = inner if isinstance(inner, dict) and inner.get("event_id_hash") is not None else response
    outcome = str(payload.get("upsert_outcome") or "").lower()
    event = {"add": "ADD", "update": "UPDATE", "rank_guarded": "NONE"}.get(outcome, "ADD")
    # For a rank-guarded write event_id_hash already carries the surviving record's id.
    anchor_id = payload.get("event_id_hash")
    if anchor_id in (None, ""):
        anchor_id = payload.get("current_memory_id") or payload.get("existing_memory_id")
    # Surface event_id_hash at the top level so result["event_id_hash"] works as a literal drop-in,
    # without dropping the gateway wrapper fields (accepted / scope / result / finalized / ...).
    if response.get("event_id_hash") in (None, "") and anchor_id not in (None, ""):
        response["event_id_hash"] = anchor_id
    if "results" not in response:
        response["results"] = [{
            "id": str(anchor_id) if anchor_id not in (None, "") else "",
            "memory": _add_memory_text(messages),
            "event": event,
        }]
    return response


def _already_normalized(messages: list) -> bool:
    """True when ``messages`` is already normalized: every element is a
    ``{role, content}`` mapping whose ``content`` is a plain string (not a list
    of provider content-parts). Such input is passed through byte-identically so
    existing callers are completely unaffected."""
    for msg in messages:
        if not isinstance(msg, dict):
            return False
        content = msg.get("content")
        if content is not None and not isinstance(content, str):
            return False
    return True


def _normalize_messages(messages: Any, provider: Optional[str] = None) -> list[Json]:
    """Normalize mem0's flexible ``messages`` into TemporalStore turns.

    * A bare **string** is wrapped as a single user turn.
    * An already-normalized ``[{role, content}, ...]`` list (string content) is
      used **as-is** — unchanged existing behavior.
    * An explicit ``provider`` (``"openai"`` / ``"anthropic"``), or any other
      shape (a provider request/response dict/object, or a list carrying
      content-part blocks), is routed through
      the built-in provider normalizer so callers can pass model SDK output directly."""
    if isinstance(messages, str):
        return [{"role": "user", "content": messages}]
    # Explicit provider always routes through the adapter.
    if provider is not None and str(provider).lower() != "auto":
        return _provider_messages(messages)
    if isinstance(messages, list):
        if _already_normalized(messages):
            return messages
        return _provider_messages(messages)
    if messages is None:
        return []
    # A provider request/response object (dict or SDK model) — sniff + adapt.
    return _provider_messages(messages)


def _scope_from_identity(user_id: Optional[str], agent_id: Optional[str], run_id: Optional[str]) -> Json:
    """Build a canonical TemporalStore scope from mem0 identity kwargs, omitting
    empty fields (``run_id`` -> ``session_id``)."""
    scope: Json = {}
    if user_id:
        scope["user_id"] = user_id
    if agent_id:
        scope["agent_id"] = agent_id
    if run_id:
        scope["session_id"] = run_id
    return scope


class Memory:
    """A subset of mem0's ``Memory`` surface backed by TemporalStore's HTTP API.

    Args:
        base_url: gateway base URL; defaults to ``$TEMPORALSTORE_BASE_URL`` /
            ``$TEMPORALSTORE_GATEWAY_URL`` / legacy ``$MATRIXARK_*`` aliases /
            ``http://127.0.0.1:8080`` (same as the ingest client).
        api_key: Bearer token; defaults to ``$TEMPORALSTORE_API_KEY`` and then
            ``$MATRIXARK_API_KEY``. When unset no
            ``Authorization`` header is sent (anonymous; fine against a dev gateway).
        timeout: per-request socket timeout in seconds.
    """

    def __init__(self, base_url: Optional[str] = None, api_key: Optional[str] = None,
                 *, timeout: float = 60.0) -> None:
        self._base_url = _resolve_base_url(base_url)
        self._api_key = _resolve_api_key(api_key)
        self._timeout = float(timeout)

    def add(self, messages: Any, *, user_id: Optional[str] = None,
            agent_id: Optional[str] = None, run_id: Optional[str] = None,
            metadata: Optional[Json] = None, provider: Optional[str] = None,
            expires_at: Optional[float] = None, ttl: Optional[float] = None,
            identity_key: Optional[str] = None, truth_class: Optional[str] = None,
            finalize: bool = True, **kw: Any) -> Json:
        """Ingest ``messages`` (mem0 ``add``). Maps ``run_id`` -> ``session_id``
        and ``agent_id`` -> ``scope.agent_id``; accepts a bare string.

        ``provider`` (optional, additive) lets a caller pass their model SDK's
        output directly: ``"openai"`` / ``"anthropic"`` force that adapter, and
        the default (``None`` / ``"auto"``) auto-sniffs provider-shaped input
        (an OpenAI ``choices`` response, an Anthropic content-block message,
        etc.) via the built-in normalizer. Already-normalized
        ``[{role, content}]`` lists are passed through unchanged.

        PurchaseMemory (additive, all optional): ``expires_at`` (absolute unix
        seconds) or ``ttl`` (relative seconds) make the memory ephemeral --
        auto-expired at read time and excluded from summaries; ``identity_key``
        + ``truth_class`` drive the keyed-upsert truth-rank guard (a higher/equal
        rank supersedes the prior value for that key; a lower rank is rejected).
        ``finalize`` (default ``True``) commits the ingest before returning, which
        is what makes ``add`` read-after-write: a ``get_all`` / ``search`` issued
        straight afterwards sees the memory, as it does on mem0. Without it the
        ingest is a streaming write that only becomes visible once a debounce
        elapses, and because every further write to the same scope pushes that
        debounce out, a burst of ``add`` calls can stay invisible for as long as
        the burst lasts -- which reads as data loss in code written against mem0.
        Pass ``finalize=False`` deliberately to stream a conversation in and let
        the idle commit close it, which is cheaper per call.

        Extra kwargs are ignored for mem0 signature compatibility."""
        normalized = _normalize_messages(messages, provider)
        # finalize so extraction runs INLINE and the memory is searchable when add returns.
        # Without it the gateway treats this as a streaming ingest and schedules an
        # idle-commit on a debounce (stream_idle_commit_timeout_ms, 1000ms by default),
        # so add -> search inside that window returns {"results": []} while get_all already
        # shows the memory. That eventual-consistency window is right for streaming
        # callers and wrong for mem0, whose add is synchronous by contract: a user
        # migrating from mem0 would conclude their memories had vanished.
        #
        # Measured: search immediately after add returned [] and returned the memory
        # 1.5s later, unchanged, purely from the debounce elapsing.
        #
        # Pass finalize=False for the streaming behaviour (cheaper add, retrievable after
        # the debounce).
        body: Json = {"kind": "message", "messages": normalized,
                      "finalize": bool(finalize)}
        scope = _scope_from_identity(user_id, agent_id, run_id)
        if scope:
            body["scope"] = scope
        if metadata:
            body["metadata"] = metadata
        if expires_at is not None:
            body["expires_at"] = expires_at
        if ttl is not None:
            body["ttl_seconds"] = ttl
        if identity_key:
            body["identity_key"] = identity_key
        if truth_class:
            body["truth_class"] = truth_class
        response = _post_json(self._base_url, self._api_key, "/memories", body, self._timeout)
        return _with_mem0_add_results(response, normalized)

    def search(self, query: str, *, user_id: Optional[str] = None,
               agent_id: Optional[str] = None, run_id: Optional[str] = None,
               limit: Optional[int] = None, raw: bool = False, **kw: Any) -> Json:
        """Retrieve context for ``query`` (mem0 ``search``). Maps identity kwargs
        to ``scope`` and caps the number of returned results. Extra kwargs are ignored.

        By default returns mem0's shape -- ``{"results": [{"id", "memory", "score",
        "metadata"}, ...]}`` -- so an existing mem0 codebase reading
        ``res["results"][i]["memory"]`` works unchanged. Pass ``raw=True`` (or call
        :meth:`search_raw`) to get the full TemporalStore response instead."""
        pack = self.search_raw(query, user_id=user_id, agent_id=agent_id, run_id=run_id, limit=limit)
        if raw:
            return pack
        return _reshape_search_results(pack)

    def _attach_real_ids(self, reshaped: Json, *, user_id: Optional[str],
                         agent_id: Optional[str], run_id: Optional[str]) -> Json:
        """Replace synthetic ``ref-N-...`` ids with the real memory id, matching on text.

        mem0's contract is that a ``search`` result is addressable: callers feed ``results[i]["id"]``
        straight back into ``get`` / ``update`` / ``delete``. A ContextPack cannot satisfy that on
        its own -- ``source_ref`` is classified as debug-only lineage and stripped from serving
        items, so every item arrives with text but no id, and ``_reshape_search_results`` has to
        synthesize one. Handing those synthesized ids back to ``get``/``update``/``delete`` fails
        (``found: false``, HTTP 500, ``deleted: false``), which is the end-to-end break this repairs.

        So the ids are recovered from ``get_all``, which does return them, by exact text match. One
        extra request, made only when a synthetic id is actually present. Items that match nothing
        keep their synthetic id: derived entity refs ("preference: drink is matcha") are projections
        of a memory rather than an addressable memory, and inventing an id for them would only move
        the failure downstream."""
        results = reshaped.get("results")
        if not isinstance(results, list) or not results:
            return reshaped
        if not any(str(entry.get("id") or "").startswith("ref-") for entry in results
                   if isinstance(entry, dict)):
            return reshaped
        try:
            listing = self.get_all(user_id=user_id, agent_id=agent_id, run_id=run_id)
        except Exception:  # A search must not fail because the id lookup did.
            return reshaped
        rows = listing.get("memories") if isinstance(listing, dict) else None
        if not isinstance(rows, list):
            rows = listing.get("results") if isinstance(listing, dict) else None
        by_text: Json = {}
        for row in rows or []:
            if not isinstance(row, dict):
                continue
            row_id = row.get("id") or row.get("memory_id")
            body = str(row.get("memory") or row.get("text") or "").strip()
            if row_id in (None, "") or not body:
                continue
            by_text.setdefault(body, row_id)
        if not by_text:
            return reshaped
        for entry in results:
            if not isinstance(entry, dict):
                continue
            if not str(entry.get("id") or "").startswith("ref-"):
                continue
            real = by_text.get(str(entry.get("memory") or "").strip())
            if real not in (None, ""):
                entry["id"] = str(real)
        return reshaped

    def search_raw(self, query: str, *, user_id: Optional[str] = None,
                   agent_id: Optional[str] = None, run_id: Optional[str] = None,
                   limit: Optional[int] = None, **kw: Any) -> Json:
        """Like :meth:`search` but returns the full TemporalStore retrieval response."""
        body: Json = {"query": query}
        scope = _scope_from_identity(user_id, agent_id, run_id)
        if scope:
            body["scope"] = scope
        if limit is not None:
            body["limit"] = int(limit)
        return _post_json(self._base_url, self._api_key, "/memories/search", body, self._timeout)

    def get(self, memory_id: str, **kw: Any) -> Json:
        """Fetch a single memory by id (mem0 ``get``). ``memory_id`` is the id returned by
        ``add`` / ``get_all``; a
        deleted/forgotten memory returns ``{"found": false}``."""
        return _get_json(self._base_url, self._api_key,
                         f"/memories/{quote(str(memory_id), safe='')}", self._timeout)

    def update(self, memory_id: str, data: str, **kw: Any) -> Json:
        """Update a memory's content (mem0 ``update``). Implemented server-side as a supersede: the
        new ``data`` is ingested in the memory's own scope and the old id is tombstoned, so a later
        ``search`` / ``get_all`` returns the new version."""
        body: Json = {"memory_id": str(memory_id), "data": data}
        return _put_json(self._base_url, self._api_key,
                         f"/memories/{quote(str(memory_id), safe='')}", body, self._timeout)

    def history(self, memory_id: str, **kw: Any) -> Json:
        """Return the ordered change history for a memory id (mem0 ``history``): ingest ->
        update/supersede -> delete, with timestamps."""
        return _get_json(self._base_url, self._api_key,
                         f"/memories/{quote(str(memory_id), safe='')}/history", self._timeout)

    def delete(self, memory_id: str, **kw: Any) -> Json:
        """Delete a single memory by id (mem0 ``delete``). ``memory_id`` is the id returned by
        ``add``/``get_all``. The addressed
        memory stops resurfacing from ``search``; provenance-closure deletion is deferred server-side."""
        return _delete_json(self._base_url, self._api_key,
                            f"/memories/{quote(str(memory_id), safe='')}", self._timeout)

    def feedback(self, memory_id: str, feedback: str, feedback_reason: Optional[str] = None,
                 **kw: Any) -> Json:
        """Rate an existing memory (mem0 ``feedback``).

        The rating is stored against the memory, not as a memory: it does not appear in `get_all`
        or `search`. Read it back with `history(memory_id)`, where it appears as an event beside
        the ingest and supersede entries.

        `feedback` is one of POSITIVE, NEGATIVE, VERY_NEGATIVE. An unknown value is refused by the
        server rather than stored.
        """
        body: Json = {"feedback": str(feedback)}
        if feedback_reason:
            body["feedback_reason"] = str(feedback_reason)
        return _post_json(self._base_url, self._api_key,
                          f"/memories/{quote(str(memory_id), safe='')}/feedback", body,
                          self._timeout)

    def batch_update(self, memories: Any, **kw: Any) -> Json:
        """Update many memories in one call (mem0 ``batch_update``).

        Takes mem0's shape: a list of dicts with ``memory_id`` (required) and ``text`` and/or
        ``metadata``. ``text`` is mem0's name for the field this API calls ``data``, so it is
        mapped; either name is accepted here.

        NOT atomic. The update
        is a supersede (ingest the amended version, tombstone the old id) with no cross-memory
        transaction behind it, so this issues one update per entry and reports what happened to
        each. Callers who need all-or-nothing must not use this. Entries are attempted in order
        and one failure does not stop the rest -- the failures are returned rather than raised, so
        a partial batch is visible instead of silently half-applied.
        """
        entries = list(memories or [])
        if len(entries) > MEM0_BATCH_LIMIT:
            raise ValueError(
                f"batch_update accepts at most {MEM0_BATCH_LIMIT} memories, got {len(entries)}"
            )
        for entry in entries:
            if not isinstance(entry, dict):
                raise ValueError("each batch_update entry must be a dict with a memory_id")
            if not entry.get("memory_id"):
                raise ValueError("each batch_update entry requires memory_id")
        return _post_json(self._base_url, self._api_key, "/memories/batch/update",
                          {"memories": entries}, self._timeout)

    def batch_delete(self, memories: Any, **kw: Any) -> Json:
        """Delete many memories in one call (mem0 ``batch_delete``).

        Takes mem0's shape: a list of dicts with ``memory_id``. A bare list of ids is accepted too,
        because it is the obvious thing to reach for and rejecting it helps nobody.

        NOT atomic, for the same reason as `batch_update`: one delete per entry, failures reported
        per memory rather than raised, so a partial batch is visible.
        """
        entries = list(memories or [])
        if len(entries) > MEM0_BATCH_LIMIT:
            raise ValueError(
                f"batch_delete accepts at most {MEM0_BATCH_LIMIT} memories, got {len(entries)}"
            )
        for entry in entries:
            memory_id = entry.get("memory_id") if isinstance(entry, dict) else entry
            if not memory_id:
                raise ValueError("each batch_delete entry requires memory_id")
        return _post_json(self._base_url, self._api_key, "/memories/batch/delete",
                          {"memories": entries}, self._timeout)

    def delete_all(self, *, user_id: Optional[str] = None, agent_id: Optional[str] = None,
                   run_id: Optional[str] = None, **kw: Any) -> Json:
        """Delete ALL memory for a subject (mem0 ``delete_all(user_id=...)``). Maps identity kwargs to
        ``scope``. Requires ``user_id``."""
        if not user_id:
            raise ValueError("delete_all requires user_id (the subject to forget)")
        scope = _scope_from_identity(user_id, agent_id, run_id)
        return _post_json(self._base_url, self._api_key, "/memories/delete_all",
                          {"scope": scope}, self._timeout)

    def delete_users(self, *, user_id: Optional[str] = None, agent_id: Optional[str] = None,
                     run_id: Optional[str] = None, **kw: Any) -> Json:
        """Forget one named subject, or EVERY subject that holds memories (mem0 ``delete_users``).

        With `user_id`, this is `delete_all` for that subject. With no identity at all, it lists
        the subjects that hold memories and forgets each one -- which is what mem0's no-argument
        `delete_users()` means.

        Two things it deliberately does not pretend about:

        * The server addresses a forget by `scope.user_id`. An agent or a run cannot be forgotten
          on its own, so an `agent_id`/`run_id`-only call raises instead of quietly deleting
          nothing. Pass the `user_id` whose memories you mean, or use `reset` for the tenant.
        * Every subject that fails is reported in `failed`, with the error. A partial wipe must
          not return looking like a complete one -- the caller is deleting data and needs to know
          which subjects still hold it.

        Returns ``{"deleted": n, "results": [...], "failed": [...]}``.
        """
        if user_id:
            return {
                "deleted": 1,
                "results": [{"user_id": user_id,
                             "result": self.delete_all(user_id=user_id, agent_id=agent_id,
                                                       run_id=run_id)}],
                "failed": [],
            }
        if agent_id or run_id:
            raise ValueError(
                "delete_users addresses a subject by user_id; an agent_id or run_id alone is not "
                "a forgettable subject. Pass the user_id whose memories you mean, or use reset()."
            )
        listed = self.users()
        rows = listed.get("results") or listed.get("items") or []
        subjects = [
            str(row.get("name") or "")
            for row in rows
            if isinstance(row, dict) and str(row.get("type") or "user") == "user" and row.get("name")
        ]
        results: list[Json] = []
        failed: list[Json] = []
        for name in subjects:
            try:
                results.append({"user_id": name, "result": self.delete_all(user_id=name)})
            except Exception as exc:  # noqa: BLE001 - report it; a silent skip reads as success.
                failed.append({"user_id": name, "error": repr(exc)})
        return {"deleted": len(results), "results": results, "failed": failed,
                "subjects_listed": len(subjects)}

    def get_all(self, *, user_id: Optional[str] = None, agent_id: Optional[str] = None,
                run_id: Optional[str] = None, limit: Optional[int] = None, **kw: Any) -> Json:
        """List a subject's active memories (mem0 ``get_all(user_id=...)``). Maps identity kwargs to
        ``scope``. Forgotten/deleted memories are excluded server-side."""
        scope = _scope_from_identity(user_id, agent_id, run_id)
        body: Json = {}
        if scope:
            body["scope"] = scope
        if limit is not None:
            body["limit"] = int(limit)
        return _post_json(self._base_url, self._api_key, "/memories/list", body, self._timeout)

    def users(self, *, limit: Optional[int] = None, **kw: Any) -> Json:
        """List the users / agents / runs that hold memories (mem0 ``users``).

        Returns mem0's shape, ``{"results": [{"type": "user", "name": "alice"}, ...]}``, with a
        ``memory_count`` added for users. "Holds memories" is the live view: a subject whose
        memories were all forgotten or have expired is not listed, which is what mem0's users()
        means -- it is not a list of provisioned accounts.
        """
        body: Json = {}
        if limit is not None:
            body["limit"] = int(limit)
        return _post_json(self._base_url, self._api_key, "/memories/users", body, self._timeout)

    def reset(self, *, confirm: str = "RESET", **kw: Any) -> Json:
        """Wipe ALL memory for the caller's tenant (mem0 ``reset``) with an
        explicit ``confirm`` (defaults to the literal ``"RESET"`` sentinel the server accepts)."""
        body: Json = {"confirm": str(confirm)}
        return _post_json(self._base_url, self._api_key, "/memories/reset", body, self._timeout)
