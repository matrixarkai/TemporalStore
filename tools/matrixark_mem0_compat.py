#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""mem0-compatible SDK shim for MatrixArk / TemporalStore.

Drop-in for ``from mem0 import Memory``. Point an existing mem0 codebase at a
MatrixArk deployment with a one-line import swap::

    # from mem0 import Memory
    from matrixark_mem0_compat import Memory

    m = Memory()                         # env-configured, same as the ingest client
    m.add(messages, user_id="u1", agent_id="assistant", run_id="sess-42")
    m.search("what did we decide?", user_id="u1", agent_id="assistant", limit=8)

mem0 passes memory identity as TOP-LEVEL kwargs (not nested under a scope). This
shim maps those kwargs onto MatrixArk's canonical ``scope`` object:

    ============  ==========================
    mem0 kwarg    MatrixArk scope field
    ============  ==========================
    ``user_id``   ``scope.user_id``
    ``agent_id``  ``scope.agent_id``
    ``run_id``    ``scope.session_id``
    ============  ==========================

The same aliases are ALSO accepted at the top level of ``/v1/ingest`` directly
(the server folds them in ``normalize_envelope``), so this shim simply builds the
canonical scope up front. Empty identity fields are omitted from the body.

Only the stdlib (``http.client`` + ``json``) is used; connection/URL/api-key
resolution is reused verbatim from ``matrixark_ingest_client`` so ``base_url`` /
``api_key`` resolve with the same env fallbacks
(``MATRIXARK_BASE_URL`` / ``MATRIXARK_GATEWAY_URL`` / ``MATRIXARK_API_KEY``).
"""
from __future__ import annotations

from typing import Any, Optional

try:  # top-level (run from tools/) ...
    from matrixark_ingest_client import (
        _post_json,
        _resolve_api_key,
        _resolve_base_url,
    )
    from matrixark_model_adapters import to_messages
except ImportError:  # ... or package path.
    from tools.matrixark_ingest_client import (  # type: ignore
        _post_json,
        _resolve_api_key,
        _resolve_base_url,
    )
    from tools.matrixark_model_adapters import to_messages  # type: ignore

Json = dict[str, Any]

__all__ = ["Memory"]


def _already_normalized(messages: list) -> bool:
    """True when ``messages`` is already MatrixArk-shaped: every element is a
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
    """Normalize mem0's flexible ``messages`` into MatrixArk turns.

    * A bare **string** is wrapped as a single user turn.
    * An already-normalized ``[{role, content}, ...]`` list (string content) is
      used **as-is** — unchanged existing behavior.
    * An explicit ``provider`` (``"openai"`` / ``"anthropic"``), or any other
      shape (a provider request/response dict/object, or a list carrying
      content-part blocks), is routed through
      ``matrixark_model_adapters.to_messages`` so customers can pass their model
      SDK's output directly."""
    if isinstance(messages, str):
        return [{"role": "user", "content": messages}]
    # Explicit provider always routes through the adapter.
    if provider is not None and str(provider).lower() != "auto":
        return to_messages(messages, provider=provider)
    if isinstance(messages, list):
        if _already_normalized(messages):
            return messages
        return to_messages(messages, provider="auto")
    if messages is None:
        return []
    # A provider request/response object (dict or SDK model) — sniff + adapt.
    return to_messages(messages, provider="auto")


def _scope_from_identity(user_id: Optional[str], agent_id: Optional[str], run_id: Optional[str]) -> Json:
    """Build a canonical MatrixArk scope from mem0 identity kwargs, omitting
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
    """A subset of mem0's ``Memory`` surface backed by MatrixArk's HTTP API.

    Args:
        base_url: gateway base URL; defaults to ``$MATRIXARK_BASE_URL`` /
            ``$MATRIXARK_GATEWAY_URL`` / ``http://127.0.0.1:8080`` (same as the
            ingest client).
        api_key: Bearer token; defaults to ``$MATRIXARK_API_KEY``. When unset no
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
            **kw: Any) -> Json:
        """Ingest ``messages`` (mem0 ``add``). Maps ``run_id`` -> ``session_id``
        and ``agent_id`` -> ``scope.agent_id``; accepts a bare string.

        ``provider`` (optional, additive) lets a caller pass their model SDK's
        output directly: ``"openai"`` / ``"anthropic"`` force that adapter, and
        the default (``None`` / ``"auto"``) auto-sniffs provider-shaped input
        (an OpenAI ``choices`` response, an Anthropic content-block message,
        etc.) via ``matrixark_model_adapters.to_messages``. Already-normalized
        ``[{role, content}]`` lists are passed through unchanged. Extra kwargs
        are ignored for mem0 signature compatibility. Returns the parsed
        ``/v1/ingest`` response."""
        body: Json = {"kind": "message",
                      "messages": _normalize_messages(messages, provider)}
        scope = _scope_from_identity(user_id, agent_id, run_id)
        if scope:
            body["scope"] = scope
        if metadata:
            body["metadata"] = metadata
        return _post_json(self._base_url, self._api_key, "/v1/ingest", body, self._timeout)

    def search(self, query: str, *, user_id: Optional[str] = None,
               agent_id: Optional[str] = None, run_id: Optional[str] = None,
               limit: Optional[int] = None, **kw: Any) -> Json:
        """Retrieve context for ``query`` (mem0 ``search``). Maps identity kwargs
        to ``scope`` and ``limit`` to ``ranking.max_selected_refs`` (the number of
        selected refs MatrixArk returns). Extra kwargs are ignored. Returns the
        parsed ``/v1/retrieve`` response."""
        body: Json = {"query": query}
        scope = _scope_from_identity(user_id, agent_id, run_id)
        if scope:
            body["scope"] = scope
        if limit is not None:
            body["ranking"] = {"max_selected_refs": int(limit)}
        return _post_json(self._base_url, self._api_key, "/v1/retrieve", body, self._timeout)
