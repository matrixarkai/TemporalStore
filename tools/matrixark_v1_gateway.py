#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Enterprise Cloud API gateway for TemporalStore: the documented `/v1/*` contract, in pure Python.

This is the deploy-ready front door for teams that ingest resources/skills and pull managed context
through APIs (rather than agent hooks). It wraps the same synchronous MatrixArk backend `server` used
by the legacy ASGI front (`matrixark_asgi.make_asgi_app`) and layers on the four things an enterprise
edge needs that the legacy front does not have: per-tenant bearer auth, token-bucket rate limiting,
request/blob quotas, and a streamed `/v1/blob/<key>` proxy to the datanode.

    uvicorn matrixark_v1_gateway:application --host 0.0.0.0 --port 8080 --workers 4

Design constraints honored here (identical to the sibling front):
  * Only TemporalStore (the storage engine) is Rust; this gateway is 100% Python, framework-free raw
    ASGI, and imports cleanly with no server library installed (uvicorn is a runtime-only dependency).
  * Durability lives in TemporalStore's async ingestion — the gateway hands off the fast, durable
    write and returns `202`; extraction/summaries/embeddings run async inside the engine.
  * The MatrixArk backend (`server.call_tool` / MCP dispatch) is synchronous, so it runs in a
    threadpool (`asyncio.to_thread`) to keep the event loop free for high concurrency.
  * The blob proxy streams request/response bodies chunk-by-chunk (bounded memory) straight to the
    datanode's `POST|PUT|GET /blob/<key>` endpoint using only `http.client` from the stdlib.

Routes: POST /v1/ingest (async fast-ack 202) · POST /v1/ingest_file (stream file body, store to the
        blob tier + ingest in one call) · POST /v1/session/commit · POST /v1/retrieve ·
        POST /v1/mcp · PUT|POST|GET /v1/blob/<key> · GET /v1/healthz|/readyz. Everything that is not
        under `/v1` is delegated to the legacy `make_asgi_app(server)` so `/api/*`, `/mcp`, `/healthz`
        keep working unchanged (back-compat).
"""
from __future__ import annotations

import asyncio
import contextvars
import gzip
import hashlib
import json
import logging
import math
import os
import random
import sys
import tempfile
import threading
import time
import uuid
from collections.abc import Mapping
from typing import Any, Awaitable, Callable, Optional, Tuple
from urllib.parse import parse_qs, unquote, urlparse
from collections.abc import Mapping

try:
    from tools.matrixark_asgi import make_asgi_app, _api_key
    from tools.matrixark_http import apply_ingest_route_defaults, mcp_http_dispatch
except ImportError:  # Direct script execution from tools/.
    from matrixark_asgi import make_asgi_app, _api_key  # type: ignore
    from matrixark_http import apply_ingest_route_defaults, mcp_http_dispatch  # type: ignore

# The write side of /v1/admin/config (a closed registry of operator-settable model settings) and the
# edge request metrics. Both are self-contained stdlib modules; a deployment missing them degrades to
# the previous read-only config surface rather than failing to import.
try:
    from tools import matrixark_deployment_plan as _deployment_plan  # type: ignore
    from tools import matrixark_gateway_config as _gwconfig  # type: ignore
    from tools import matrixark_gateway_metrics as _gwmetrics  # type: ignore
except ImportError:  # Direct script execution from tools/.
    import matrixark_deployment_plan as _deployment_plan  # type: ignore
    import matrixark_gateway_config as _gwconfig  # type: ignore
    import matrixark_gateway_metrics as _gwmetrics  # type: ignore

# Canonical tool -> required-scopes map. The SAME map the backend gates with
# (matrixark_access.MatrixArkAccessManager.authenticate), so the edge per-tool gate on /v1/mcp
# mirrors the backend exactly: unmapped tool -> empty set -> no scope requirement. Falls back to an
# empty map so the gateway keeps importing cleanly where the core module is unavailable (the mcp
# per-tool gate then degrades to "no tool scope", still behind the valid-key check in _authorize).
try:  # pragma: no cover - trivial import shim
    try:
        from tools.matrixark_mcp_core import MATRIXARK_TOOL_SCOPES  # type: ignore
    except ImportError:
        from matrixark_mcp_core import MATRIXARK_TOOL_SCOPES  # type: ignore
except Exception:  # pragma: no cover
    MATRIXARK_TOOL_SCOPES: dict[str, set[str]] = {}

# Key hashing: reuse the backend's `secret_hash` (sha256 hex) so a key minted by the provisioner /
# backend credential store verifies identically at the edge. Falls back to a self-contained sha256
# so the gateway keeps importing cleanly even where the backend identity module is unavailable.
try:  # pragma: no cover - trivial import shim
    try:
        from tools.matrixark_mcp_core_identity import secret_hash as _secret_hash  # type: ignore
    except ImportError:
        from matrixark_mcp_core_identity import secret_hash as _secret_hash  # type: ignore
except Exception:  # pragma: no cover
    import hashlib as _hashlib

    def _secret_hash(value: str) -> str:
        return _hashlib.sha256(value.encode("utf-8")).hexdigest()

Json = dict[str, Any]

_LOG = logging.getLogger("matrixark.gateway")

# Fires the no-auth startup warning at most once per process.
_AUTH_WARNED = {"done": False}

# Set by `_warn_if_auth_disabled` at startup, which is where the posture is already known.
# Absent means this process never asked, and the snapshot then says nothing rather than guessing.
_AUTH_POSTURE: Dict[str, bool] = {}

_NO_AUTH_WARNING = (
    "MatrixArk gateway is running WITHOUT authentication (dev default). Anyone who "
    "can reach this address has full anonymous access and there is NO tenant "
    "isolation. Set MATRIXARK_REQUIRE_AUTH=1 (and MATRIXARK_ACCESS_MODE=enforced) "
    "to enforce API keys."
)


def _warn_if_auth_disabled(cfg: "GatewayConfig") -> None:
    """Emit a one-time, NON-BLOCKING warning when auth is effectively off
    (``require_auth`` False). Never rejects requests or blocks startup — behavior
    stays fully anonymous-allowed; this only surfaces the posture to the operator.

    Also records the posture for the configuration snapshot, which has no ``GatewayConfig`` of its
    own. ``require_auth`` can be set by the config dict as well as the environment, so reading the
    environment there would be a guess -- and a guess about whether a deployment is open is worse
    than saying nothing. Recorded in BOTH branches: a process that ran this and found auth ON is
    just as much evidence as one that found it off."""
    _AUTH_POSTURE["require_auth"] = bool(getattr(cfg, "require_auth", False))
    if getattr(cfg, "require_auth", False):
        return
    if _AUTH_WARNED["done"]:
        return
    _AUTH_WARNED["done"] = True
    _LOG.warning("WARNING: %s", _NO_AUTH_WARNING)


# ---- defaults (all overridable via env or the `config` dict) -----------------------------------
_MIB = 1024 * 1024
_GIB = 1024 * 1024 * 1024
# DEV DEFAULT: auth is OFF out of the box so the API works anonymously with zero
# config. This is a deliberate developer-experience default; production MUST opt in
# to auth with MATRIXARK_REQUIRE_AUTH=1 (and MATRIXARK_ACCESS_MODE=enforced). When
# require_auth is False the gateway logs a one-time startup warning (see
# _warn_if_auth_disabled). The enforced path is unchanged: set require_auth True and
# bad/missing keys -> 401 with tenant/account pinned from the key hash.
_DEFAULTS = {
    "require_auth": False,
    "enforced": False,
    "ingest_rps": 5000.0,
    "ingest_burst": 10000.0,
    "retrieve_rps": 6000.0,
    "retrieve_burst": 12000.0,
    "blob_streams": 500,
    "max_body_bytes": 16 * _MIB,
    "max_batch": 1000,
    "max_blob_bytes": 5 * _GIB,
    # Directory the /v1/ingest_file route spools the streamed body to before hashing
    # (keeps memory flat for large files). Empty -> the system temp dir.
    "ingest_spool_dir": "",
    "datanode_url": "http://127.0.0.1:17102",
    "blob_timeout": 30.0,
    "backend_timeout": 30.0,   # gateway-level cap on a single backend call (s)
    # Direct network path (B): when enabled, /v1/ingest and /v1/retrieve POST a
    # high-level {scope, messages}/{scope, query} body over a shared async
    # connection pool to `direct_backend_url` (the Rust service proxy by default,
    # or a datanode for one hop less), bypassing the serial stdio MCP adapter.
    # The gateway does NO hashing -- the proxy owns tenant_hash + shard routing.
    "direct_backend": False,
    "direct_backend_url": "http://127.0.0.1:17000",
    "direct_pool_size": 64,
    # Streaming-ingest debounce: a plain (non-finalize) /v1/ingest schedules a native
    # idle-commit task with this deadline so the background materializer can drain it and
    # the ingest becomes retrievable on its own. Must be > 0 (0 would commit inline and
    # defeat the debounce); <= 0 disables scheduling (retrieve-time flush still applies).
    "stream_idle_commit_timeout_ms": 1000,
    # Per-key usage metering (edge). An in-process counter is bumped after a key is validated on
    # each AUTHENTICATED request; it is flushed to `usage_file` (JSONL/JSON snapshot) at most every
    # `usage_flush_every` recorded requests OR `usage_flush_interval_s` seconds, whichever comes
    # first. Empty `usage_file` keeps the counter fully in-process (snapshot still readable via the
    # /v1/admin/api_key_usage route). Metering runs ONLY in enforced mode (a real key exists); dev/
    # anonymous requests are never metered. Best-effort: a metering failure never breaks a request.
    "usage_file": "",
    "usage_flush_every": 50,
    "usage_flush_interval_s": 5.0,
}

# route -> (tool name, rate-limit class). "__mcp__" is dispatched through mcp_http_dispatch.
_DATA_ROUTES: dict[str, Tuple[str, str]] = {
    "/v1/ingest": ("matrixark_ingest", "ingest"),
    "/v1/session/commit": ("matrixark_session_commit", "ingest"),
    "/v1/retrieve": ("matrixark_retrieve", "retrieve"),
    "/v1/mcp": ("__mcp__", "retrieve"),
    # Memory management (mem0 conformance). forget/delete/reset gate on `context:forget` (see
    # _CATEGORY_SCOPE); get_all (POST /v1/memories) gates on `context:retrieve`. GET /v1/memories is
    # handled by a dedicated branch below (data routes are POST-only).
    "/v1/forget": ("matrixark_forget", "forget"),
    "/v1/delete": ("matrixark_delete", "forget"),
    "/v1/reset": ("matrixark_reset", "forget"),
    "/v1/memories": ("matrixark_get_all", "retrieve"),
    # mem0 users(): which users/agents/runs hold memories. A read, gated like get_all.
    "/v1/users": ("matrixark_list_users", "retrieve"),
    # One resource/skill's stored text, paged. A read, gated like the other listings.
    "/v1/resource/content": ("matrixark_get_resource_content", "retrieve"),
    # update = supersede (ingest an amended version + tombstone the old id): gates on context:ingest.
    "/v1/update": ("matrixark_update_memory", "ingest"),
    # mem0 feedback(): rate an existing memory. A write about a memory, so it gates like one; the
    # rating is read back through GET /v1/memory/<id>/history, not as a memory of its own.
    "/v1/memory/feedback": ("matrixark_memory_feedback", "ingest"),
    # get (GET /v1/memory/<id>) and history (GET /v1/memory/<id>/history) are handled by dedicated
    # GET branches below (data routes are POST-only), gated on context:retrieve.
}

# Backend exception class names we translate to specific edge status codes (matched by name to avoid
# a hard import dependency on the backend package).
_BACKPRESSURE_ERRORS = {"MatrixArkBackpressureError"}
# The addressed thing does not exist -- the caller's own state to fix, not a server fault. Matched
# by class name, like the sets above, so a message that merely contains "not found" is unaffected.
_NOT_FOUND_ERRORS = {"MatrixArkNotFoundError"}
# The request is malformed -- the caller's to fix, and not worth a retry.
_INVALID_REQUEST_ERRORS = {"MatrixArkInvalidRequestError"}
_STORAGE_QUOTA_ERRORS = {
    "MatrixArkStorageQuotaError",
    "StorageQuotaExceeded",
    "MatrixArkStorageQuotaExceeded",
}


# ================================================================================================
# Config
# ================================================================================================
def _env_bool(value: Any, default: bool) -> bool:
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _parse_api_keys(env: Any, overrides: Json) -> dict[str, str]:
    """`{api_key: tenant}`. Precedence: explicit override dict > *_FILE (JSON) > *_KEYS csv."""
    if isinstance(overrides.get("api_keys"), dict):
        return {str(k): str(v) for k, v in overrides["api_keys"].items()}
    path = str(env.get("MATRIXARK_API_KEYS_FILE", "") or "").strip()
    if path:
        with open(path, "r", encoding="utf-8") as handle:
            data = json.load(handle)
        if isinstance(data, dict):
            return {str(k): str(v) for k, v in data.items()}
    raw = str(env.get("MATRIXARK_API_KEYS", "") or "").strip()
    keys: dict[str, str] = {}
    for pair in raw.split(","):
        pair = pair.strip()
        if not pair:
            continue
        if ":" in pair:
            key, tenant = pair.split(":", 1)
            keys[key.strip()] = tenant.strip()
        else:  # a bare key isolates under a namespace named after itself.
            keys[pair] = pair
    return keys


def _str_list(value: Any) -> list[str]:
    """Coerce a keystore field into a ``list[str]`` (non-list/absent -> empty list)."""
    if isinstance(value, list):
        return [str(item) for item in value]
    return []


def _opt_int(value: Any) -> Optional[int]:
    """Coerce a keystore field into an ``int`` or ``None`` (absent/invalid -> None).

    ``bool`` is rejected (it is an ``int`` subclass but never a meaningful quota). This is the
    backward-compatibility hinge for ``request_quota``: a legacy record with no such field yields
    ``None`` -> UNLIMITED, byte-identical to today."""
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    if isinstance(value, str) and value.strip().lstrip("-").isdigit():
        return int(value.strip())
    return None


def _opt_float(value: Any) -> Optional[float]:
    """Coerce a keystore field into a ``float`` or ``None`` (absent/invalid -> None)."""
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value.strip())
        except ValueError:
            return None
    return None


def _normalize_key_record(raw: Json, *, default_account: str = "") -> Json:
    """Normalize a keystore value into the canonical EDGE key record shape.

    Carries the authorization fields all the way to the edge so the dispatcher can enforce them:
    ``tenant_id``/``account_id`` (identity, as before) plus ``scopes``/``allowed_user_ids``/
    ``allowed_session_ids``/``role``.

    ``scopes`` semantics are the backward-compatibility hinge:
      * ``None`` (the field is ABSENT from the source, i.e. the LEGACY plain
        ``{sha256:{tenant_id,account_id}}`` form) means UNRESTRICTED -- no scope enforcement, so a
        legacy keystore behaves exactly as it did before this change.
      * a present list (even empty) is authoritative -- the key may only use those scopes.
    ``allowed_user_ids``/``allowed_session_ids`` default to ``[]`` meaning NO user/session restriction.
    """
    scopes_raw = raw.get("scopes")
    if scopes_raw is None:
        scopes: Optional[list[str]] = None
    elif isinstance(scopes_raw, list):
        scopes = [str(item) for item in scopes_raw]
    else:  # a bare scalar scope -> single-element list
        scopes = [str(scopes_raw)]
    return {
        "tenant_id": str(raw.get("tenant_id") or ""),
        "account_id": str(raw.get("account_id") or "") or default_account,
        "scopes": scopes,
        "allowed_user_ids": _str_list(raw.get("allowed_user_ids")),
        "allowed_session_ids": _str_list(raw.get("allowed_session_ids")),
        "role": str(raw.get("role") or ""),
        # Per-key request QUOTA, carried to the edge like scopes. ``request_quota`` None/<=0 ->
        # UNLIMITED (backward compatible: a legacy record has no such field). ``quota_window`` is the
        # rolling window in seconds; None/<=0 -> a per-process lifetime window (never resets).
        "request_quota": _opt_int(raw.get("request_quota")),
        "quota_window": _opt_float(raw.get("quota_window")),
    }


def _parse_hashed_keys(env: Any, overrides: Json) -> dict[str, Json]:
    """Enforced-mode keystore: ``{api_key_hash -> record}``.

    Keys are stored HASHED, never plaintext. The store is provisioned by
    ``matrixark_provision_api_key.py`` (which reuses the backend ``secret_hash`` and the
    ``matrixark_api_key`` record shape). Two source shapes are accepted from
    ``MATRIXARK_API_KEYS_HASHED_FILE``:

      * JSONL of ``matrixark_api_key`` records (same fields the backend credential store writes:
        ``record_type``/``api_key_hash``/``account_id``/``tenant_id``/``status``/``expires_at_ms``,
        plus ``scopes``/``allowed_user_ids``/``allowed_session_ids``/``role``).
      * a plain JSON object ``{"<sha256hex>": {"tenant_id": ..., "account_id": ...}}`` (LEGACY form,
        no ``scopes`` -> UNRESTRICTED, backward compatible).

    Each stored value is normalized (see ``_normalize_key_record``) so the edge dispatcher can enforce
    the key's ``scopes``/``allowed_user_ids``/``allowed_session_ids``. ``overrides["hashed_api_keys"]``
    (a dict) short-circuits for tests. Inactive/expired records are skipped; the last active record
    for a given hash wins.
    """
    if isinstance(overrides.get("hashed_api_keys"), dict):
        out: dict[str, Json] = {}
        for k, v in overrides["hashed_api_keys"].items():
            raw = dict(v) if isinstance(v, dict) else {"tenant_id": str(v)}
            out[str(k)] = _normalize_key_record(raw)
        return out
    path = str(env.get("MATRIXARK_API_KEYS_HASHED_FILE", "") or "").strip()
    out = {}
    if not path or not os.path.exists(path):
        return out
    now = int(time.time() * 1000)

    def _add(hash_hex: str, raw: Json) -> None:
        if hash_hex and str(raw.get("tenant_id") or ""):
            out[hash_hex] = _normalize_key_record(raw, default_account="acct_local")

    with open(path, "r", encoding="utf-8") as handle:
        text = handle.read()
    stripped = text.strip()
    if stripped.startswith("{") and '"api_key_hash"' not in stripped:
        # plain JSON object form (LEGACY): value is {tenant_id, account_id} or a bare tenant string.
        try:
            data = json.loads(stripped)
        except json.JSONDecodeError:
            data = {}
        if isinstance(data, dict):
            for hash_hex, value in data.items():
                if isinstance(value, dict):
                    _add(str(hash_hex), value)
                else:
                    _add(str(hash_hex), {"tenant_id": str(value)})
        return out
    # JSONL of records (append-only credential-store shape).
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(record, dict) or record.get("record_type") not in (None, "matrixark_api_key"):
            continue
        if str(record.get("status", "active")) != "active":
            out.pop(str(record.get("api_key_hash") or ""), None)
            continue
        expires_at_ms = record.get("expires_at_ms")
        if isinstance(expires_at_ms, int) and expires_at_ms <= now:
            continue
        _add(str(record.get("api_key_hash") or ""), record)
    return out


class GatewayConfig:
    """Resolved gateway configuration (env + optional overrides)."""

    def __init__(self, **fields: Any) -> None:
        for name, value in _DEFAULTS.items():
            setattr(self, name, fields.get(name, value))
        self.api_keys: dict[str, str] = fields.get("api_keys", {})
        # Enforced-mode hashed keystore: {api_key_hash -> {tenant_id, account_id}}.
        self.hashed_keys: dict[str, Json] = fields.get("hashed_keys", {})
        # Datanode blob target, parsed for the http.client proxy.
        parsed = urlparse(str(self.datanode_url))
        self.blob_scheme = parsed.scheme or "http"
        self.blob_host = parsed.hostname or "127.0.0.1"
        self.blob_port = parsed.port or (443 if self.blob_scheme == "https" else 17102)
        # Injectable so tests can proxy without a network; production builds an http.client conn.
        self.blob_connection_factory: Callable[["GatewayConfig"], Any] = fields.get(
            "blob_connection_factory", _default_blob_connection
        )
        # Direct-backend target (proxy or datanode) parsed for the pooled client.
        # `direct_connection_factory` is injectable so tests stub it with no network.
        self.direct_connection_factory: Optional[Callable[[], Any]] = fields.get(
            "direct_connection_factory"
        )

    @classmethod
    def from_env(cls, overrides: Optional[Json] = None) -> "GatewayConfig":
        overrides = dict(overrides or {})
        env = os.environ

        def num(env_name: str, key: str, cast: Callable[[Any], Any]) -> Any:
            if key in overrides:
                return cast(overrides[key])
            raw = env.get(env_name)
            return cast(raw) if raw not in (None, "") else _DEFAULTS[key]

        return cls(
            api_keys=_parse_api_keys(env, overrides),
            hashed_keys=_parse_hashed_keys(env, overrides),
            require_auth=_env_bool(
                overrides.get("require_auth", env.get("MATRIXARK_REQUIRE_AUTH")),
                _DEFAULTS["require_auth"],
            ),
            enforced=_env_bool(
                overrides.get("enforced", env.get("MATRIXARK_AUTH_ENFORCED")),
                _DEFAULTS["enforced"],
            ),
            ingest_rps=num("MATRIXARK_RL_INGEST_RPS", "ingest_rps", float),
            ingest_burst=num("MATRIXARK_RL_INGEST_BURST", "ingest_burst", float),
            retrieve_rps=num("MATRIXARK_RL_RETRIEVE_RPS", "retrieve_rps", float),
            retrieve_burst=num("MATRIXARK_RL_RETRIEVE_BURST", "retrieve_burst", float),
            blob_streams=num("MATRIXARK_RL_BLOB_STREAMS", "blob_streams", int),
            max_body_bytes=num("MATRIXARK_QUOTA_MAX_BODY_BYTES", "max_body_bytes", int),
            max_batch=num("MATRIXARK_QUOTA_MAX_BATCH", "max_batch", int),
            max_blob_bytes=num("MATRIXARK_QUOTA_MAX_BLOB_BYTES", "max_blob_bytes", int),
            ingest_spool_dir=str(
                overrides.get("ingest_spool_dir")
                or env.get("MATRIXARK_INGEST_SPOOL_DIR")
                or _DEFAULTS["ingest_spool_dir"]
            ),
            datanode_url=str(
                overrides.get("datanode_url")
                or env.get("MATRIXARK_DATANODE_BLOB_URL")
                or env.get("MATRIXARK_DATANODE_URL")
                or _DEFAULTS["datanode_url"]
            ),
            blob_timeout=num("MATRIXARK_BLOB_TIMEOUT_S", "blob_timeout", float),
            backend_timeout=num("MATRIXARK_GATEWAY_BACKEND_TIMEOUT_MS", "backend_timeout", lambda v: float(v) / 1000.0),
            blob_connection_factory=overrides.get("blob_connection_factory", _default_blob_connection),
            direct_backend=_env_bool(
                overrides.get("direct_backend", env.get("MATRIXARK_GATEWAY_DIRECT_BACKEND")),
                _DEFAULTS["direct_backend"],
            ),
            direct_backend_url=str(
                overrides.get("direct_backend_url")
                or env.get("MATRIXARK_GATEWAY_DIRECT_URL")
                or _DEFAULTS["direct_backend_url"]
            ),
            direct_pool_size=num("MATRIXARK_GATEWAY_DIRECT_POOL", "direct_pool_size", int),
            direct_connection_factory=overrides.get("direct_connection_factory"),
            stream_idle_commit_timeout_ms=num(
                "MATRIXARK_STREAM_IDLE_COMMIT_TIMEOUT_MS", "stream_idle_commit_timeout_ms", int
            ),
            usage_file=str(
                overrides.get("usage_file")
                or env.get("MATRIXARK_API_KEY_USAGE_FILE")
                or _DEFAULTS["usage_file"]
            ),
            usage_flush_every=num("MATRIXARK_API_KEY_USAGE_FLUSH_EVERY", "usage_flush_every", int),
            usage_flush_interval_s=num(
                "MATRIXARK_API_KEY_USAGE_FLUSH_INTERVAL_S", "usage_flush_interval_s", float
            ),
        )


def _coerce_config(config: Any) -> GatewayConfig:
    if isinstance(config, GatewayConfig):
        return config
    # `isinstance` cannot recognise a class python loaded twice. tools/ is importable both flat
    # (`matrixark_v1_gateway`) and as a package (`tools.matrixark_v1_gateway`), and those are two
    # module objects with two unrelated GatewayConfig classes. A config built through one spelling
    # fails `isinstance` against the other, falls through to from_env, and raises there on
    # `dict(overrides or {})` -- "'GatewayConfig' object is not iterable".
    #
    # Each test passes alone, because only one spelling is loaded; the whole suite loads both.
    #
    # What this function needs to know is whether it was handed a BUILT config or a mapping to
    # build one from, and that does not depend on which module object the class came from.
    if config is not None and not isinstance(config, Mapping) and hasattr(config, "api_keys"):
        return config
    return GatewayConfig.from_env(config or {})


def _default_blob_connection(cfg: GatewayConfig) -> Any:
    import http.client

    if cfg.blob_scheme == "https":
        return http.client.HTTPSConnection(cfg.blob_host, cfg.blob_port, timeout=cfg.blob_timeout)
    return http.client.HTTPConnection(cfg.blob_host, cfg.blob_port, timeout=cfg.blob_timeout)


# ================================================================================================
# Rate limiting (token bucket per key+class; concurrent-stream cap for blob)
# ================================================================================================
class _TokenBucket:
    """Thread-safe token bucket. Refills at `rps` tokens/sec up to `capacity` (burst)."""

    def __init__(self, rps: float, burst: float) -> None:
        self.rps = float(rps)
        self.capacity = float(burst)
        self.tokens = float(burst)
        self.last = time.monotonic()
        self.lock = threading.Lock()

    def take(self) -> Tuple[bool, int, float, float]:
        """Return (allowed, remaining, reset_seconds, retry_after_seconds)."""
        with self.lock:
            now = time.monotonic()
            elapsed = now - self.last
            if elapsed > 0 and self.rps > 0:
                self.tokens = min(self.capacity, self.tokens + elapsed * self.rps)
                self.last = now
            if self.tokens >= 1.0:
                self.tokens -= 1.0
                reset = 0.0 if self.rps <= 0 else (self.capacity - self.tokens) / self.rps
                return True, int(self.tokens), reset, 0.0
            retry = 0.0 if self.rps <= 0 else (1.0 - self.tokens) / self.rps
            return False, 0, retry, retry


class _RateLimiter:
    def __init__(self, cfg: GatewayConfig) -> None:
        self.cfg = cfg
        self._buckets: dict[Tuple[str, str], _TokenBucket] = {}
        self._lock = threading.Lock()
        streams = int(cfg.blob_streams)
        self._blob_sem = threading.BoundedSemaphore(streams) if streams > 0 else None

    def _params(self, cls: str) -> Tuple[float, float]:
        if cls == "ingest":
            return self.cfg.ingest_rps, self.cfg.ingest_burst
        return self.cfg.retrieve_rps, self.cfg.retrieve_burst

    def check(self, key: str, cls: str) -> Tuple[bool, int, float, float]:
        bkey = (key, cls)
        with self._lock:
            bucket = self._buckets.get(bkey)
            if bucket is None:
                rps, burst = self._params(cls)
                bucket = _TokenBucket(rps, burst)
                self._buckets[bkey] = bucket
        return bucket.take()

    def headers(self, cls: str, remaining: int, reset: float) -> list[Tuple[bytes, bytes]]:
        rps, _ = self._params(cls)
        return [
            (b"x-ratelimit-limit", str(int(rps)).encode()),
            (b"x-ratelimit-remaining", str(int(remaining)).encode()),
            (b"x-ratelimit-reset", str(int(math.ceil(reset))).encode()),
        ]

    def blob_acquire(self) -> bool:
        if self._blob_sem is None:
            return True
        return self._blob_sem.acquire(blocking=False)

    def blob_release(self) -> None:
        if self._blob_sem is not None:
            try:
                self._blob_sem.release()
            except ValueError:
                pass


# ================================================================================================
# Per-key usage metering (edge, in-process, best-effort)
# ================================================================================================
class _UsageMeter:
    """Cheap, thread-safe, per-API-key request counter kept in-process at the edge.

    Bumped once per AUTHENTICATED request AFTER the key is validated. The hot path does only an
    O(1) dict update under a short lock -- NO disk I/O per request. The in-memory counters are
    flushed to `path` (an atomic JSON snapshot) at most every `flush_every` recorded requests OR
    `flush_interval_s` seconds, so disk cost is amortized far off the request path. An empty `path`
    disables the flush entirely (the snapshot stays purely in memory, still readable by the admin
    read route). Keys are stored HASHED (sha256), never in plaintext.

    Every method is defensive: a metering failure must NEVER surface to the request, so `record`
    swallows its own errors and the caller additionally wraps it (belt and suspenders).
    """

    def __init__(self, path: str = "", *, flush_every: int = 50, flush_interval_s: float = 5.0) -> None:
        self.path = str(path or "")
        self.flush_every = max(1, int(flush_every))
        self.flush_interval_s = max(0.0, float(flush_interval_s))
        self._lock = threading.Lock()
        self._counters: dict[str, Json] = {}
        # Rolling-window state for per-key QUOTA, kept OUT of `_counters` so the JSON snapshot stays
        # clean: {key_hash -> [window_start_monotonic, count_in_window]}. Only touched when a request
        # actually carries a positive `window_s`; lifetime-window quotas use `_counters[...]["total"]`.
        self._windows: dict[str, list] = {}
        self._dirty = 0
        self._last_flush = time.monotonic()

    def record(self, key_hash: str, tenant: Optional[str], account: Optional[str],
               category: str, nbytes: int = 0, *, window_s: float = 0.0) -> Optional[Tuple[int, float]]:
        """Bump the per-key counter and return ``(window_used, reset_seconds)`` for quota checks.

        ``window_used`` is the request count in the current window AFTER counting this request:
        the rolling window of ``window_s`` seconds when ``window_s > 0``, else the cumulative
        ``total`` (a per-process lifetime window). ``reset_seconds`` is the time until the rolling
        window rolls over (0.0 for the lifetime window). Returns ``None`` only on an empty key or an
        internal error -- metering (and therefore the quota decision) is always best-effort."""
        if not key_hash:
            return None
        try:
            with self._lock:
                entry = self._counters.get(key_hash)
                now = int(time.time() * 1000)
                if entry is None:
                    entry = {
                        "api_key_hash": key_hash,
                        "tenant_id": str(tenant or ""),
                        "account_id": str(account or ""),
                        "total": 0,
                        "ingest": 0,
                        "retrieve": 0,
                        "other": 0,
                        "bytes": 0,
                        "first_used_at_ms": now,
                        "last_used_at_ms": now,
                    }
                    self._counters[key_hash] = entry
                entry["total"] += 1
                bucket = category if category in ("ingest", "retrieve") else "other"
                entry[bucket] += 1
                entry["bytes"] += max(0, int(nbytes or 0))
                entry["last_used_at_ms"] = now
                if tenant and not entry.get("tenant_id"):
                    entry["tenant_id"] = str(tenant)
                if account and not entry.get("account_id"):
                    entry["account_id"] = str(account)
                # Rolling-window bookkeeping for quota (O(1); only when a positive window is given).
                if window_s and window_s > 0:
                    now_m = time.monotonic()
                    window = self._windows.get(key_hash)
                    if window is None or (now_m - window[0]) >= window_s:
                        window = [now_m, 0]
                        self._windows[key_hash] = window
                    window[1] += 1
                    window_used = int(window[1])
                    reset_s = max(0.0, float(window_s) - (now_m - window[0]))
                else:
                    window_used = int(entry["total"])
                    reset_s = 0.0
                self._dirty += 1
                if self._should_flush_locked():
                    self._flush_locked()
                return window_used, reset_s
        except Exception:  # pragma: no cover - metering is best-effort, never fatal
            return None
        return None

    def _should_flush_locked(self) -> bool:
        if not self.path:
            return False
        if self._dirty >= self.flush_every:
            return True
        return (time.monotonic() - self._last_flush) >= self.flush_interval_s

    def _flush_locked(self) -> None:
        if not self.path:
            self._dirty = 0
            self._last_flush = time.monotonic()
            return
        try:
            snapshot = {
                "record_type": "matrixark_api_key_usage_snapshot",
                "updated_at_ms": int(time.time() * 1000),
                "keys": list(self._counters.values()),
            }
            tmp = f"{self.path}.tmp.{os.getpid()}"
            with open(tmp, "w", encoding="utf-8") as handle:
                json.dump(snapshot, handle, sort_keys=True)
            os.replace(tmp, self.path)
        except Exception:  # pragma: no cover - flush is best-effort
            pass
        finally:
            self._dirty = 0
            self._last_flush = time.monotonic()

    def snapshot(self) -> list[Json]:
        with self._lock:
            return [dict(entry) for entry in self._counters.values()]


def _meter_active(cfg: GatewayConfig, key: Optional[str]) -> bool:
    """Metering runs only when a REAL key authenticated the request (enforced mode). Dev/anonymous
    (unenforced) traffic is never metered, so the dev default posture is byte-identical."""
    return bool(getattr(cfg, "enforced", False) and key)


def _meter_safe(meter: _UsageMeter, cfg: GatewayConfig, key: Optional[str],
                tenant: Optional[str], account: Optional[str], category: str, nbytes: int = 0,
                *, window_s: float = 0.0) -> Optional[Tuple[int, float]]:
    """Record one authenticated request; return ``(window_used, reset_s)`` or ``None`` when the
    request is not metered (dev/anonymous) or on any error. Wrapped so a metering failure can never
    break a request."""
    try:
        if not _meter_active(cfg, key):
            return None
        return meter.record(_secret_hash(key), tenant, account, category, nbytes, window_s=window_s)
    except Exception:  # pragma: no cover - defensive: metering must not affect the response
        return None


def _meter_and_check_quota(
    meter: _UsageMeter, cfg: GatewayConfig, key: Optional[str], record: Optional[Json],
    tenant: Optional[str], account: Optional[str], category: str, nbytes: int = 0,
) -> Optional[Tuple[Json, list[Tuple[bytes, bytes]]]]:
    """Meter one authenticated request and, if the key is now OVER its ``request_quota``, return the
    ``(payload, headers)`` for a 429 ``quota_exceeded`` response; else ``None`` (request proceeds).

    Enforced-mode + a real key only: dev/anonymous traffic is never metered, so ``_meter_safe``
    returns ``None`` and this returns ``None`` (dev posture byte-identical). A key with no
    ``request_quota`` (or 0/None) is UNLIMITED. The compare is O(1) against the in-memory counter the
    meter already maintains. The ``request_quota``-th request in a window is the last allowed; the
    next one (count > limit) is rejected. Fully best-effort: ANY error returns ``None`` so a
    quota-check bug can neither crash the hot path nor wrongly block a legitimate request."""
    try:
        window_s = 0.0
        if record is not None:
            qw = record.get("quota_window")
            if isinstance(qw, (int, float)) and not isinstance(qw, bool) and qw > 0:
                window_s = float(qw)
        metered = _meter_safe(meter, cfg, key, tenant, account, category, nbytes, window_s=window_s)
        if metered is None or record is None:
            return None
        limit = record.get("request_quota")
        if not isinstance(limit, int) or isinstance(limit, bool) or limit <= 0:
            return None
        used, reset_s = metered
        if used <= limit:
            return None
        retry = int(math.ceil(reset_s)) if reset_s and reset_s > 0 else 0
        payload: Json = {"error": "quota_exceeded", "limit": limit, "used": used}
        headers = [
            (b"retry-after", str(retry).encode()),
            (b"x-ratelimit-quota-limit", str(limit).encode()),
            (b"x-ratelimit-quota-remaining", b"0"),
            (b"x-ratelimit-quota-reset", str(retry).encode()),
        ]
        return payload, headers
    except Exception:  # pragma: no cover - defensive: a quota bug must never block/crash a request
        return None


# The admin scopes that may read per-key usage. Either grants the read (mirrors the backend, where
# `matrixark_admin_*` require `admin:api_key` and audit reads require `admin:audit`).
_USAGE_READ_SCOPES = {"admin:api_key", "admin:audit"}


def _usage_rows_visible_to(record: Optional[Json], rows: list, tenant: Optional[str],
                           account: Optional[str]) -> list:
    """The usage rows this caller may see.

    The meter's snapshot is deployment-wide -- every metered key's hash, tenant, account, request
    counts and byte volume -- and the route returned it whole, so one tenant's admin key read how
    much traffic every other tenant was doing.

    A scoped enforced key sees its own account and tenant. A dev key (no record) or a legacy
    unrestricted key (``scopes is None``) is unchanged, which is the same posture
    ``_usage_read_denied`` takes for those keys.
    """
    if record is None or record.get("scopes") is None:
        return rows
    return [row for row in rows
            if row.get("tenant_id") == tenant and row.get("account_id") == account]


_AUDIT_READ_SCOPES = {"admin:audit"}


def _audit_read_denied(record: Optional[Json]) -> Optional[Json]:
    """403 payload when the key may not read the audit log, else ``None``.

    Narrower than ``_usage_read_denied`` on purpose. That gate admits ``admin:api_key`` as well,
    which is right for usage counters -- a key manager needs to see what their keys are doing. The
    audit log is the record of who reached for what and was refused, the catalogue publishes
    ``admin:audit`` as "Read the audit log", and a scope that names one thing should be the thing
    that opens it. Same dev/legacy posture as its neighbours.
    """
    if record is None:
        return None
    scopes = record.get("scopes")
    if scopes is None:
        return None
    if _AUDIT_READ_SCOPES.intersection(set(scopes)):
        return None
    return {"error": "insufficient_scope", "required": sorted(_AUDIT_READ_SCOPES)}


def _audit_recording_mode() -> str:
    """What this worker is doing with audit records right now.

    Reported, not acted on: an empty log means "nothing happened" or "nothing is kept", and only
    this tells them apart. The value is frozen for the MCP server at its construction, so what this
    reports is the mode a change would take effect under after a restart -- which is what the
    setting's own help says.
    """
    return (os.environ.get("MATRIXARK_AUDIT_MODE", "off").strip().lower() or "off")


_ADMIN_WRITE_SCOPES = {"admin:api_key"}


def _admin_write_denied(record: Optional[Json]) -> Optional[Json]:
    """403 payload when the key may not CHANGE anything, else ``None``.

    Same shape and the same dev/legacy posture as ``_usage_read_denied``, over a narrower set. The
    scope catalogue this gateway serves calls ``admin:audit`` "Read the audit log", and it was
    authorising configuration writes and ingestion. A scope presented as read-only has to be
    read-only, or the label is the lie.
    """
    if record is None:
        return None
    scopes = record.get("scopes")
    if scopes is None:  # legacy/unrestricted key
        return None
    if _ADMIN_WRITE_SCOPES.intersection(scopes):
        return None
    return {"error": "insufficient_scope", "required": sorted(_ADMIN_WRITE_SCOPES)}


def _usage_read_denied(record: Optional[Json]) -> Optional[Json]:
    """403 payload when the key may not read usage, else ``None``.

    Consistent with the rest of the edge: a dev key (``record is None``) or a legacy/unrestricted key
    (``scopes is None``) is allowed; a scoped enforced-mode key must carry ``admin:api_key`` or
    ``admin:audit``.
    """
    if record is None:
        return None
    scopes = record.get("scopes")
    if scopes is None:  # legacy/unrestricted key
        return None
    if _USAGE_READ_SCOPES.intersection(scopes):
        return None
    return {"error": "insufficient_scope", "required": sorted(_USAGE_READ_SCOPES)}


# ================================================================================================
# Auth + tenant isolation
# ================================================================================================
def _authorize(headers: list[Tuple[bytes, bytes]], cfg: GatewayConfig) -> Tuple[
        bool, Optional[str], Optional[str], Optional[str], Optional[Json]]:
    """Resolve the Bearer key to a tenant identity (and, in enforced mode, its key record).

    Returns ``(allowed, api_key, tenant_id, account_id, key_record)``. ``key_record`` is the matched,
    normalized enforced-mode keystore record (carrying ``scopes``/``allowed_user_ids``/
    ``allowed_session_ids``/``role``) so the dispatcher can enforce per-key authorization; it is
    ``None`` in dev/unenforced mode (and for anonymous access), which means NO scope/user enforcement.

    ENFORCED mode (``MATRIXARK_AUTH_ENFORCED=1``): EVERY /v1 request must carry a Bearer key whose
    sha256 hash is present in the provisioned hashed keystore. A missing, unknown, or revoked key --
    and the legacy demo key ``sk_live_demo`` (which is never hashed into the enforced store) -- is
    rejected (401). The identity ``{tenant_id, account_id}`` is taken from the stored record, NEVER
    from client-supplied free text, so two tenants cannot collide on a shared scope string.

    DEV/unenforced mode (default): unchanged legacy behavior -- the plaintext ``api_keys`` env map is
    honored and, when ``require_auth`` is off, anonymous requests are allowed. This keeps existing dev
    flows (the demo key) working when enforcement is off.
    """
    key = _api_key(headers)
    if cfg.enforced:
        if not key:
            return False, None, None, None, None
        record = cfg.hashed_keys.get(_secret_hash(key))
        if record:
            return (True, key, str(record.get("tenant_id") or ""),
                    str(record.get("account_id") or "") or None, record)
        return False, None, None, None, None
    # ---- dev / unenforced (legacy) --------------------------------------------------------------
    if key and key in cfg.api_keys:
        return True, key, cfg.api_keys[key], None, None
    if not cfg.require_auth:  # local/dev: anonymous is allowed.
        return True, key, (cfg.api_keys.get(key) if key else None) or "anonymous", None, None
    return False, None, None, None, None


# route -> required MatrixArk scope. The ingest/retrieve rate-limit CLASS from `_DATA_ROUTES` doubles
# as the scope selector: an ingest-category route needs `context:ingest`, a retrieve-category route
# needs `context:retrieve`.
_CATEGORY_SCOPE = {"ingest": "context:ingest", "retrieve": "context:retrieve", "forget": "context:forget"}


def _required_scope(path: str, method: str, route: Optional[Tuple[str, str]]) -> Optional[str]:
    """The MatrixArk scope a request to `path`/`method` requires, or ``None`` (no scope needed).

    Blob writes (PUT|POST /v1/blob/<key>) and combined upload-ingest (POST /v1/ingest_file) are
    ingest; blob reads (GET /v1/blob/<key>) are retrieve. Data routes (/v1/ingest, /v1/retrieve,
    /v1/session/commit, /v1/mcp) map through their `_DATA_ROUTES` category. Health/readyz => None.
    """
    if path.startswith("/v1/blob/"):
        if method in ("PUT", "POST"):
            return "context:ingest"
        if method == "GET":
            return "context:retrieve"
        return None
    if path == "/v1/ingest_file":
        return "context:ingest"
    if route is not None:
        return _CATEGORY_SCOPE.get(route[1])
    return None


def _scope_denied(record: Optional[Json], required_scope: Optional[str]) -> Optional[Json]:
    """403 payload when the key's ``scopes`` list does not permit ``required_scope``, else ``None``.

    Enforcement runs ONLY when ``record`` is present (enforced mode + a matched key) AND the key has a
    non-``None`` ``scopes`` list. ``scopes=None`` (legacy keystore) or no required scope => allowed.
    """
    if record is None or required_scope is None:
        return None
    scopes = record.get("scopes")
    if scopes is None:  # legacy/unrestricted key
        return None
    if required_scope not in scopes:
        return {"error": "insufficient_scope", "required": required_scope}
    return None


def _identity_denied(record: Optional[Json], args: Json) -> Optional[Json]:
    """403 payload when the key's ``allowed_user_ids``/``allowed_session_ids`` exclude the request's
    ``scope.user_id``/``scope.session_id`` (checked AFTER identity is applied), else ``None``.

    An empty allow-list (the default) imposes NO restriction on that axis -- unchanged behavior.
    """
    if record is None:
        return None
    scope = args.get("scope")
    scope = scope if isinstance(scope, dict) else {}
    allowed_users = record.get("allowed_user_ids") or []
    if allowed_users and str(scope.get("user_id") or "") not in allowed_users:
        return {"error": "user_not_allowed"}
    allowed_sessions = record.get("allowed_session_ids") or []
    if allowed_sessions and str(scope.get("session_id") or "") not in allowed_sessions:
        return {"error": "session_not_allowed"}
    return None


def _mcp_denied(record: Optional[Json], parsed: Json) -> Optional[Json]:
    """403 payload for a ``/v1/mcp`` JSON-RPC request the key may not make, else ``None``.

    Replaces the old blanket ``context:retrieve`` gate on ``/v1/mcp`` with the SAME per-tool scope
    map the backend enforces (``MATRIXARK_TOOL_SCOPES``), so the edge is no longer coarser than the
    engine: a data-only key can no longer reach ``matrixark_admin_*`` tools (which require ``admin:*``)
    through the MCP route.

    Semantics (mirroring ``matrixark_access`` + the sibling ``_scope_denied``/``_identity_denied``):
      * Enforcement runs ONLY for an enforced-mode key with a non-``None`` ``scopes`` list. A dev key
        (``record is None``) or a legacy plain keystore key (``scopes is None``) is UNRESTRICTED --
        ``/v1/mcp`` stays byte-identical to today for those.
      * Only ``tools/call`` carries a tool; ``initialize`` / ``tools/list`` / ``ping`` / notifications
        require no tool scope (still gated by a valid key via ``_authorize``).
      * ``required = MATRIXARK_TOOL_SCOPES.get(name, set())``. Non-empty and NOT a subset of the key's
        scopes -> 403 ``insufficient_scope`` (``required`` sorted for a stable payload). Unmapped tool
        -> empty required -> allowed (matches the backend's ``.get(tool_name, set())``).
      * User/session: ``allowed_user_ids``/``allowed_session_ids`` are applied (via ``_identity_denied``)
        against ``params.arguments.scope`` ONLY when the call actually carries a ``scope`` object --
        a scopeless ``tools/call`` imposes no user/session restriction (nothing to check).
      * FALLBACK: a ``tools/call`` with no usable tool name (missing/blank ``params.name``) cannot be
        mapped, so it falls back to the historical coarse ``context:retrieve`` requirement rather than
        passing unchecked or crashing.
    """
    if record is None:
        return None
    if record.get("scopes") is None:  # legacy/unrestricted key
        return None
    if not isinstance(parsed, dict) or parsed.get("method") != "tools/call":
        return None
    params = parsed.get("params")
    params = params if isinstance(params, dict) else {}
    name = params.get("name")
    if not isinstance(name, str) or not name:
        # Unparseable / tool-less call -> historical coarse gate, not a free pass.
        return _scope_denied(record, "context:retrieve")
    scopes = set(record.get("scopes") or [])
    required = MATRIXARK_TOOL_SCOPES.get(name, set())
    if required and not required.issubset(scopes):
        return {"error": "insufficient_scope", "required": sorted(required)}
    margs = params.get("arguments")
    margs = margs if isinstance(margs, dict) else {}
    if isinstance(margs.get("scope"), dict):
        return _identity_denied(record, margs)
    return None


def _apply_identity(args: Json, key: Optional[str], tenant: Optional[str],
                    account: Optional[str] = None) -> Json:
    """Inject identity and namespace-isolate `scope` under the tenant (and account).

    The shared backend access manager validates `scope` as an OBJECT (dict) for *every* backend
    (`optional_object(args, "scope")`), so the tenant is injected as `scope["tenant_id"]` (and the
    resolved `scope["account_id"]`). Injecting a bare tenant *string* (an earlier behavior) made the
    backend reject every ingest/retrieve with "scope must be an object".

    The tenant identity is authoritative: the session/pending buffer key derives from
    `(account_id, tenant_id, user_id, session_id)`, so pinning both `tenant_id` and `account_id` from
    the authenticated key -- rather than trusting the client's `scope` string -- is what isolates one
    tenant's memory (and the `pending_async_event` fallback buffer) from another's.

    The END-USER identity (`user_id`/`session_id`) is a different axis and is NOT server-pinned: the
    tenant's own backend supplies it per request, so it is PRESERVED from the client `scope` here --
    only `tenant_id`/`account_id` are (re)written, never `user_id`/`session_id`. That is what keeps
    two end-users of the SAME tenant on distinct buffer keys (e.g. `alice` cannot read `bob`'s
    pending/session buffer even under one shared API key). A tenant that wants per-user isolation MUST
    pass `user_id` (and, for per-session isolation, `session_id`) in `scope`; when both are absent the
    request legitimately falls back to the tenant-level buffer (`user_id`/`session_id` default to "").

    The edge bearer key is forwarded as the backend `api_key` only when
    MATRIXARK_GATEWAY_FORWARD_API_KEY is truthy (default on). Deployments where the edge itself is the
    trust boundary and the backend runs in `dev` access mode set it to 0, so the edge token is not
    misread as a backend credential.
    """
    if key and _env_bool(os.environ.get("MATRIXARK_GATEWAY_FORWARD_API_KEY"), True):
        args["api_key"] = key
    if tenant:
        args["tenant"] = tenant
        scope = args.get("scope")
        if isinstance(scope, dict):
            scope["tenant_id"] = tenant
            if account:
                scope["account_id"] = account
        elif isinstance(scope, str) and scope:
            label = scope if (scope == tenant or scope.startswith(tenant + "/")) else f"{tenant}/{scope}"
            new_scope: Json = {"tenant_id": tenant, "namespace": label}
            if account:
                new_scope["account_id"] = account
            args["scope"] = new_scope
        else:
            new_scope = {"tenant_id": tenant}
            if account:
                new_scope["account_id"] = account
            args["scope"] = new_scope
    return args


def _isolate_key(key_str: str, tenant: Optional[str], account: Optional[str] = None) -> str:
    if not tenant:
        return key_str
    prefix = f"{account}/{tenant}" if account else tenant
    if key_str == prefix or key_str.startswith(prefix + "/"):
        return key_str
    return f"{prefix}/{key_str}"


# ================================================================================================
# ASGI helpers
# ================================================================================================
# The Accept-Encoding of the request being served.
#
# `_json` is called from 197 places and none of them have the scope, so threading it through would
# be 197 edits to reach one header. A context variable is set once per request instead: asyncio
# copies the context for each task, so one request cannot read another's, and a call made outside a
# request (a test calling the helper directly) sees the default and compresses nothing.
_ACCEPT_ENCODING: "contextvars.ContextVar[str]" = contextvars.ContextVar(
    "matrixark_accept_encoding", default="")

# gzip has an envelope, and three of the eleven admin reads are smaller than it. Below this a
# response would get bigger and cost CPU to do it.
_JSON_PACK_FLOOR = 1024


# What an admin page is allowed to do, and who is allowed to embed it.
#
# The pages need very little: nothing is fetched cross-origin except by explicit configuration,
# there are no inline event handlers, and there is no eval or new Function anywhere -- so
# 'unsafe-eval' is not needed and default-src can be 'self'.
#
# 'unsafe-inline' IS needed: every page carries its stylesheet and scripts inline. So this policy
# does NOT stop injected inline script, and saying otherwise would be the more dangerous mistake.
# What it does stop is the page being framed, script being pulled from another origin, a <base>
# tag redirecting every relative URL, and a form posting somewhere else.
#
# connect-src is open on purpose. The key portal lets a customer point at a management host and a
# gateway host of their own; a policy that forbade that would break a documented feature to close
# a hole this deployment does not have.
_CONTENT_SECURITY_POLICY = (
    "default-src 'self'; "
    "script-src 'self' 'unsafe-inline'; "
    "style-src 'self' 'unsafe-inline'; "
    "img-src 'self' data:; "
    "connect-src *; "
    "object-src 'none'; "
    "base-uri 'none'; "
    "form-action 'none'; "
    "frame-ancestors 'none'"
)

# Sent with every response, HTML or JSON. nosniff because a response read as a different type is
# how a JSON body becomes executable; no-referrer because an admin URL names the deployment and
# there is nowhere it needs to travel to.
_SAFETY_HEADERS = [
    (b"x-content-type-options", b"nosniff"),
    (b"referrer-policy", b"no-referrer"),
]

# frame-ancestors is the modern spelling and X-Frame-Options the one older intermediaries honour.
# The portal has destructive controls one click behind a confirm box; being framed is the attack
# that turns a confirm box into a decoration.
_HTML_SAFETY_HEADERS = _SAFETY_HEADERS + [
    (b"content-security-policy", _CONTENT_SECURITY_POLICY.encode("ascii")),
    (b"x-frame-options", b"DENY"),
]


# A backend failure goes to the log; the caller gets a token that names it.
#
# `except Exception` catches whatever the backend raised -- an OSError naming a store directory, a
# connection error naming an internal host and port, a driver message quoting the statement it
# choked on. That text was returned to whoever made the call, including a service-key holder who is
# not an operator here, and it was recorded nowhere: sanitising it alone would have closed the leak
# by destroying the only copy, so both halves are done together.
_GATEWAY_LOG = logging.getLogger("matrixark.gateway")

# The incident minted while serving this request, if one was.
#
# It is minted inside a handler and the request is recorded in the wrapper's `finally`, after that
# handler has returned, so something has to carry it the few frames up. A context variable does --
# the same way the request's Accept-Encoding reaches the response helpers -- and asyncio copies the
# context per task, so one request cannot read another's.
_INCIDENT: "contextvars.ContextVar[str]" = contextvars.ContextVar(
    "matrixark_incident", default="")

# What the caller is told instead. Deliberately about the call and not about the deployment: which
# storage this runs on and where it lives is not the caller's business.
_FAILURE_DETAIL = {
    "backend_error": "The backend could not complete this call.",
    "backend_unavailable": "The backend could not be reached.",
    "storage_quota_exceeded": "The storage quota for this deployment is exhausted.",
    "settings_unavailable": "The settings registry could not be read.",
    "extraction_failed": "Extraction did not finish; the write itself is durable.",
}


def _incident(scope: Json, code: str, exc: BaseException) -> str:
    """Record one failure and return the token that names it.

    ERROR level, so it is visible without configuring anything: with no handler installed Python
    writes WARNING and above to stderr through its last-resort handler, which is where a container's
    logs already go. A deployment that wants them elsewhere configures the `matrixark.gateway`
    logger and this keeps working.
    """
    token = uuid.uuid4().hex[:12]
    _INCIDENT.set(token)
    _GATEWAY_LOG.error("%s %s -> %s [incident %s]", scope.get("method", "?"),
                       scope.get("path", "?"), code, token, exc_info=exc)
    return token


def _failure(scope: Json, code: str, exc: BaseException) -> Json:
    """The body for a failure the caller must not be told the inside of."""
    return {
        "error": code,
        "detail": _FAILURE_DETAIL.get(code, _FAILURE_DETAIL["backend_error"]),
        "incident": _incident(scope, code, exc),
    }


async def _json(send: Callable, status: int, payload: Json,
                extra_headers: Optional[list[Tuple[bytes, bytes]]] = None) -> None:
    data = json.dumps(payload).encode("utf-8")
    headers = [(b"content-type", b"application/json")]
    if len(data) >= _JSON_PACK_FLOOR and _accepts_gzip(_ACCEPT_ENCODING.get()):
        # Not cached: these are dynamic reads, and a cache keyed by content would grow an entry per
        # distinct answer. The pages can be held because there are seven of them.
        data = gzip.compress(data, 6, mtime=0)
        headers.append((b"content-encoding", b"gzip"))
    # Whether this response is compressed depends on the request, and a shared cache that ignored
    # that would hand a gzip body to a client that asked for none.
    headers.append((b"vary", b"accept-encoding"))
    # Configuration, audit records and per-key usage all come through here. Nothing between the
    # gateway and the browser should be keeping any of it.
    headers.append((b"cache-control", b"no-store"))
    headers.extend(_SAFETY_HEADERS)
    headers.append((b"content-length", str(len(data)).encode()))
    if extra_headers:
        headers.extend(extra_headers)
    await send({"type": "http.response.start", "status": status, "headers": headers})
    await send({"type": "http.response.body", "body": data})


async def _text(send: Callable, status: int, body: str,
                content_type: str = "text/plain; charset=utf-8",
                extra_headers: Optional[list[Tuple[bytes, bytes]]] = None) -> None:
    """A plain-text response. Used for the Prometheus exposition format, which needs its own
    content type rather than JSON or HTML."""
    data = body.encode("utf-8")
    headers = [(b"content-type", content_type.encode()),
               (b"content-length", str(len(data)).encode())]
    if extra_headers:
        headers.extend(extra_headers)
    await send({"type": "http.response.start", "status": status, "headers": headers})
    await send({"type": "http.response.body", "body": data})


# Compressed portal pages, keyed by the identity validator of the page they came from. Seven
# pages, 490 KB uncompressed and 137 KB packed, so this holds ~137 KB per worker and takes 353 KB
# off every full tour of the portal. Bounded by construction -- there are seven pages -- but a
# ceiling anyway, because the key is content-derived and a deployment that reloads pages would
# otherwise accumulate one entry per version.
_HTML_PACKED: dict[str, bytes] = {}
_HTML_PACKED_MAX = 32
# Below this a gzip envelope is a meaningful share of the payload and the round trip dominates
# anything it could save. The error fallbacks are the only HTML this size.
_HTML_PACK_FLOOR = 1024


def _validator(body: bytes) -> str:
    """A strong ETag for these bytes. Content-derived, so it is stable across workers and across
    restarts -- a validator that changed per process would 200 on every revalidation from a
    deployment behind more than one worker."""
    return '"' + hashlib.sha256(body).hexdigest()[:32] + '"'


def _accepts_gzip(header: str) -> bool:
    """Whether the client actually wants gzip.

    ``gzip;q=0`` is a refusal, and ``identity;q=0, *`` is consent to anything. Both are lost by
    looking for the substring, which is how a client that asked NOT to be compressed gets a
    compressed body it will not decode.
    """
    for part in header.split(","):
        token, _semi, params = part.strip().partition(";")
        token = token.strip().lower()
        if token not in ("gzip", "*"):
            continue
        quality = 1.0
        for param in params.split(";"):
            name, _eq, value = param.strip().partition("=")
            if name.strip().lower() == "q":
                try:
                    quality = float(value)
                except ValueError:
                    quality = 0.0
        if quality > 0:
            return True
    return False


def _packed(body: bytes, validator: str) -> bytes:
    packed = _HTML_PACKED.get(validator)
    if packed is None:
        # mtime=0: the default stamps the current time into the header, so the same page would
        # compress to different bytes each process and anything hashing the response would see a
        # change that is not one.
        packed = gzip.compress(body, 6, mtime=0)
        if len(_HTML_PACKED) >= _HTML_PACKED_MAX:
            _HTML_PACKED.clear()
        _HTML_PACKED[validator] = packed
    return packed


def _matches(header: str, validator: str) -> bool:
    """Does the client already hold this exact representation?

    ``*`` matches anything, and a weak validator (``W/"..."``) still identifies the same bytes for
    the purpose of a conditional GET.
    """
    if not header:
        return False
    for candidate in header.split(","):
        candidate = candidate.strip()
        if candidate == "*":
            return True
        if candidate.startswith("W/"):
            candidate = candidate[2:].strip()
        if candidate == validator:
            return True
    return False


async def _page(send: Callable, scope: Json, body: bytes) -> None:
    """One bundled portal page: compressed if the client takes it, revalidated if they have it.

    ``no-cache`` is a revalidation instruction, not a refusal to cache -- the client keeps the
    page and asks whether it still holds, which is a 304 with no body. Serving these stale would
    show a customer a portal that does not match the gateway they upgraded, so revalidating every
    time and paying nothing when nothing moved is the trade that fits.
    """
    headers = _headers_map(scope)
    validator = _validator(body)
    encoding = None
    if len(body) >= _HTML_PACK_FLOOR and _accepts_gzip(headers.get("accept-encoding", "")):
        body = _packed(body, validator)
        encoding = b"gzip"
        # A different representation needs a different validator, or a client holding the
        # identity copy revalidates with this one and is told to reuse bytes it cannot read.
        validator = validator[:-1] + '-gzip"'

    if _matches(headers.get("if-none-match", ""), validator):
        await send({"type": "http.response.start", "status": 304, "headers": [
            (b"etag", validator.encode()),
            (b"cache-control", b"no-cache"),
            (b"vary", b"accept-encoding"),
        ]})
        return await send({"type": "http.response.body", "body": b""})

    extra = [(b"etag", validator.encode()),
             (b"cache-control", b"no-cache"),
             # Without this a shared cache can hand a compressed body to a client that cannot
             # take one, having stored it under a key that ignored the difference.
             (b"vary", b"accept-encoding")]
    if encoding:
        extra.append((b"content-encoding", encoding))
    return await _html(send, 200, body, extra)


async def _html(send: Callable, status: int, body: bytes,
                extra_headers: Optional[list[Tuple[bytes, bytes]]] = None) -> None:
    headers = [(b"content-type", b"text/html; charset=utf-8"),
               (b"content-length", str(len(body)).encode())]
    headers.extend(_HTML_SAFETY_HEADERS)
    if extra_headers:
        headers.extend(extra_headers)
    await send({"type": "http.response.start", "status": status, "headers": headers})
    await send({"type": "http.response.body", "body": body})


# The customer-facing key-management portal page. Served (static, no auth) from GET /v1/admin/portal;
# every ACTION button on the page calls an admin-gated JSON endpoint, so the page is inert without a
# valid admin key. The canonical source is the committed file `tools/portal/api_key_portal.html`
# (single source of truth); the page is read from disk once and cached per process. A tiny fallback
# keeps the route working (and still pointing operators at the real endpoints) if the file is absent.
_PORTAL_HTML_CACHE: dict[str, Optional[bytes]] = {"bytes": None}
_PORTAL_FALLBACK_HTML = (
    "<!doctype html><meta charset='utf-8'><title>MatrixArk API Key Portal</title>"
    "<h1>MatrixArk API Key Portal</h1>"
    "<p>The bundled portal page (<code>tools/portal/api_key_portal.html</code>) was not found on this "
    "deployment. Key management still works directly against the admin JSON endpoints: "
    "<code>POST /api/admin/create_api_key</code>, <code>POST /api/admin/list_api_keys</code>, "
    "<code>POST /api/admin/rotate_api_key</code>, <code>POST /api/admin/revoke_api_key</code>, and "
    "<code>GET /v1/admin/api_key_usage</code> — each with an admin-scoped "
    "<code>Authorization: Bearer &lt;key&gt;</code>.</p>"
)


_INGESTION_PORTAL_CACHE: dict[str, Optional[bytes]] = {"bytes": None}


def _ingestion_portal_html_bytes() -> bytes:
    """The ingestion portal page (cached), read from the committed file next to this module."""
    cached = _INGESTION_PORTAL_CACHE.get("bytes")
    if cached is not None:
        return cached
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "portal", "ingestion_portal.html")
    try:
        with open(path, "rb") as handle:
            data = handle.read()
    except Exception:  # pragma: no cover - deployments without the file bundled
        data = (
            "<!doctype html><meta charset='utf-8'><title>MatrixArk Ingestion</title>"
            "<h1>MatrixArk Ingestion</h1><p>The bundled page "
            "(<code>tools/portal/ingestion_portal.html</code>) was not found. The JSON endpoints "
            "still work: <code>POST /v1/admin/ingestion/jobs</code>, "
            "<code>GET /v1/admin/ingestion/jobs</code>, and <code>GET /v1/metrics</code>.</p>"
        ).encode("utf-8")
    _INGESTION_PORTAL_CACHE["bytes"] = data
    return data


_SETUP_PORTAL_CACHE: dict[str, Optional[bytes]] = {"bytes": None}
_CATALOG_PORTAL_CACHE: dict[str, Optional[bytes]] = {"bytes": None}


def _portal_page(cache: dict, filename: str, fallback: str) -> bytes:
    """A bundled portal page (cached per process), read from tools/portal/ next to this module."""
    cached = cache.get("bytes")
    if cached is not None:
        return cached
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "portal", filename)
    try:
        with open(path, "rb") as handle:
            data = handle.read()
    except Exception:  # pragma: no cover - deployments without the file bundled
        data = fallback.encode("utf-8")
    cache["bytes"] = data
    return data


_OVERVIEW_PORTAL_CACHE: dict[str, Optional[bytes]] = {"bytes": None}
_EXPLORE_PORTAL_CACHE: dict[str, Optional[bytes]] = {"bytes": None}


def _overview_portal_html_bytes() -> bytes:
    return _portal_page(
        _OVERVIEW_PORTAL_CACHE, "overview_portal.html",
        "<!doctype html><meta charset='utf-8'><title>MatrixArk</title><h1>MatrixArk</h1>"
        "<p>The bundled page (<code>tools/portal/overview_portal.html</code>) was not found. "
        "<a href='/v1/admin/setup'>Setup</a> · <a href='/v1/admin/catalog'>Catalog</a> · "
        "<a href='/v1/admin/ingestion'>Ingestion</a> · <a href='/v1/admin/portal'>Keys</a></p>")


def _explore_portal_html_bytes() -> bytes:
    return _portal_page(
        _EXPLORE_PORTAL_CACHE, "explore_portal.html",
        "<!doctype html><meta charset='utf-8'><title>MatrixArk Explore</title>"
        "<h1>MatrixArk Explore</h1><p>The bundled page "
        "(<code>tools/portal/explore_portal.html</code>) was not found. The JSON endpoints still "
        "work: <code>POST /v1/retrieve</code>, <code>GET /v1/memories</code>, "
        "<code>GET /v1/users</code>, and <code>POST /v1/ingest</code>.</p>")


_API_PORTAL_CACHE: dict[str, Optional[bytes]] = {"bytes": None}


def _api_portal_html_bytes() -> bytes:
    return _portal_page(
        _API_PORTAL_CACHE, "api_portal.html",
        "<!doctype html><meta charset='utf-8'><title>MatrixArk API</title><h1>MatrixArk API</h1>"
        "<p>The bundled page (<code>tools/portal/api_portal.html</code>) was not found. The same "
        "list is served as JSON at <code>GET /v1/admin/routes</code>.</p>")


def _setup_portal_html_bytes() -> bytes:
    return _portal_page(
        _SETUP_PORTAL_CACHE, "setup_portal.html",
        "<!doctype html><meta charset='utf-8'><title>MatrixArk Setup</title>"
        "<h1>MatrixArk Setup</h1><p>The bundled page (<code>tools/portal/setup_portal.html</code>) "
        "was not found. The JSON endpoints still work: <code>GET|POST /v1/admin/config</code>, "
        "<code>POST /v1/admin/config/preset</code>, and <code>POST /v1/admin/config/test</code>.</p>")


def _catalog_portal_html_bytes() -> bytes:
    return _portal_page(
        _CATALOG_PORTAL_CACHE, "catalog_portal.html",
        "<!doctype html><meta charset='utf-8'><title>MatrixArk Catalog</title>"
        "<h1>MatrixArk Catalog</h1><p>The bundled page "
        "(<code>tools/portal/catalog_portal.html</code>) was not found. The JSON endpoints still "
        "work: <code>GET /v1/skills</code>, <code>GET /v1/resources</code>, and "
        "<code>POST /v1/resource/content</code>.</p>")


def _portal_html_bytes() -> bytes:
    """The portal HTML (cached). Reads the committed file next to this module; falls back to a small
    inline notice page so the route always returns valid HTML."""
    cached = _PORTAL_HTML_CACHE.get("bytes")
    if cached is not None:
        return cached
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "portal", "api_key_portal.html")
    try:
        with open(path, "rb") as handle:
            data = handle.read()
    except Exception:  # pragma: no cover - fallback for deployments without the file bundled
        data = _PORTAL_FALLBACK_HTML.encode("utf-8")
    _PORTAL_HTML_CACHE["bytes"] = data
    return data


_STORE_SENTINELS = {"", "local", "none", "standalone", "off"}


# Encoders this deployment can be pointed at, with what they measured on a 298-pair retrieval
# benchmark built from real documentation. The numbers are here so an operator choosing a model sees
# the trade rather than guessing from parameter counts, which do not predict the ranking: the 560M
# model wins by about a point and a half at six times the cost, the 278M model wins nothing, and the
# widest-window model scores worst of all. Dimension truncation matters as much as model size --
# e5-large scores HIGHER truncated to 512 than at its native 1024.
#
# These are measurements on ONE corpus of technical documentation with English and Chinese queries.
# They rank these models on that corpus; they do not promise the same ranking on a different one,
# which is why the portal presents them as evidence rather than as a recommendation to apply blindly.
# Encoders a HOSTED provider serves. Deliberately not rows in _ENCODER_CATALOG: every entry there
# carries hit@1 and throughput measured by encoding a real corpus on this machine, which is only
# possible for a model we run. Giving these numbers they do not have would put invented figures in
# the comparison table, sorted and marked "best" against measured ones -- so they are named here and
# offered as choices, and the table stays what it is.
#
# Each name is one the code already uses: the encoder's own default for that provider, or a preset.
_HOSTED_ENCODERS = [
    {"model": "voyage-3", "serves": "voyage", "dim": 1024,
     "note": "Voyage's general encoder, and what this build asks for when the provider is Voyage."},
    {"model": "text-embedding-3-small", "serves": "openai", "dim": 1536,
     "note": "OpenAI's cheaper encoder; what the OpenAI preset configures."},
    {"model": "text-embedding-3-large", "serves": "openai", "dim": 3072,
     "note": "OpenAI's stronger encoder, and what this build asks for on an OpenAI-compatible "
             "endpoint that names no model."},
]

_ENCODER_CATALOG = [
    {
        "id": "intfloat/multilingual-e5-large",
        "label": "multilingual-e5-large @512 dims",
        "params_m": 560, "dims": 512, "window": 512,
        "hit_at_1": 76.2, "hit_at_5": 92.3, "texts_per_s": 1.7,
        "vectors_mb_per_doc": 10.70, "needs_prefix": True, "recommended": True,
        "note": "Best measured retrieval of the set. Truncating its 1024 values to 512 scores "
                "HIGHER than leaving them full (76.2 against 74.2 hit@1) while halving vector "
                "memory. It encodes six times slower than e5-small, which matters far less when "
                "embedding is deferred and backfilled behind the import.",
    },
    {
        "id": "intfloat/multilingual-e5-small",
        "label": "multilingual-e5-small",
        "params_m": 118, "dims": 384, "window": 512,
        "hit_at_1": 74.8, "hit_at_5": 90.6, "texts_per_s": 10.7,
        "vectors_mb_per_doc": 8.02, "needs_prefix": True, "recommended": True,
        "note": "Best quality per unit of cost, and within about a point and a half of e5-large on "
                "both metrics -- roughly the measurement's own error bar -- at six times the "
                "throughput and a quarter less vector memory. The right default when the import "
                "window is the constraint.",
    },
    {
        "id": "intfloat/multilingual-e5-base",
        "label": "multilingual-e5-base",
        "params_m": 278, "dims": 768, "window": 512,
        "hit_at_1": 74.8, "hit_at_5": 91.3, "texts_per_s": 4.8,
        "vectors_mb_per_doc": 16.05, "needs_prefix": True, "recommended": False,
        "note": "Sits between the two on quality and cost without winning either. Its edge over "
                "e5-small comes from the wider vector, not the parameter count: truncated to 384 "
                "it drops to 71.5%.",
    },
    {
        "id": "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2",
        "label": "MiniLM-L12 (current default)",
        "params_m": 118, "dims": 384, "window": 512,
        "hit_at_1": 59.4, "hit_at_5": 82.2, "texts_per_s": 31.7,
        "vectors_mb_per_doc": 8.02, "needs_prefix": False, "recommended": False,
        "note": "Fastest of the set and the weakest by a wide margin -- fifteen points of hit@1 "
                "below e5-small at identical size and memory. It remains the default only because "
                "existing stores were embedded with it and switching requires a backfill.",
    },
    {
        "id": "BAAI/bge-m3",
        "label": "bge-m3 (long context)",
        "params_m": 568, "dims": 1024, "window": 8192,
        "hit_at_1": 49.7, "hit_at_5": 70.8, "texts_per_s": 0.2,
        "vectors_mb_per_doc": 2.42, "needs_prefix": False, "recommended": False,
        "note": "An 8192-token window needs far fewer vectors -- 2.42 MB per document against 8.02 "
                "-- but it scored worst on retrieval and encodes over a hundred times slower than "
                "e5-small. Consider it only when vector storage is the binding constraint.",
    },
]


def encoder_catalog() -> list:
    """The catalog, with the active model marked, for the portal's model picker."""
    active = os.environ.get("MATRIXARK_EMBEDDING_MODEL", "").strip()
    out = []
    for entry in _ENCODER_CATALOG:
        item = dict(entry)
        item["active"] = bool(active) and active == entry["id"]
        out.append(item)
    return out


def _encoder_applies(provider: str) -> bool:
    """Whether a SELF-HOSTED encoder is something this provider could run.

    An OpenAI-compatible endpoint is the one value covering two worlds -- OpenAI itself and a local
    server behind the same protocol, which is what the MiniLM preset configures -- so it keeps them.
    A hosted-only provider cannot serve a repository id at all.
    """
    effect = _gwconfig.embedding_provider_effect(provider)
    if effect == "hash":
        return True  # nothing chosen yet; narrowing towards nothing helps no one
    if effect == "local_model":
        return True
    return (provider or "").strip().lower() != "voyage"


def _hosted_encoders_for(provider: str) -> List[Json]:
    """Encoders the provider serves itself, with no measurement to show for them.

    An in-process encoder gets NONE: it loads a model rather than calling one, so a hosted name is
    not a thing it could be pointed at. Nothing chosen yet gets all of them, because narrowing
    towards nothing helps no one.
    """
    name = (provider or "").strip().lower()
    effect = _gwconfig.embedding_provider_effect(name)
    if effect == "local_model":
        return []
    if effect != "api":
        return [dict(row) for row in _HOSTED_ENCODERS]
    wanted = "voyage" if name == "voyage" else "openai"
    return [dict(row) for row in _HOSTED_ENCODERS if row["serves"] == wanted]


def _model_picker_body(target: str) -> Json:
    """The part of /v1/admin/models that needs no network: which setting a pick belongs in, what is
    in that setting now, and the suggestions for the selected provider.

    Which setting is decided HERE, not in the page. On Anthropic it is extraction.anthropic_model,
    and the page used to write every pick into extraction.model -- a field the Anthropic path never
    reads, so a pick filled in a form, said it was set, and changed nothing.

    Split out so a test can call it. Rebuilding this in a test instead would leave the decision
    tested twice and shipped once, which is exactly what a mutation of the route proved: the route
    could report the wrong field with every assertion still green.
    """
    # One extraction field now, and it routes itself: `_env_name` sends it to the variable the
    # selected provider reads. There is nothing left for this to choose between.
    setting_key = "embedding.model" if target == "embedding" else "extraction.model"
    body: Json = {
        "target": target,
        "key": setting_key,
        "catalogue": (embedding_picker_catalogue() if target == "embedding"
                      else _gwconfig.model_catalogue(target)),
        # Resolved, not read off `.env`: the extraction model has no fixed variable any more, so
        # `.env` is empty for it and this read would answer "" for every deployment.
        "current": os.environ.get(
            _gwconfig._env_name(_gwconfig.SETTINGS_BY_KEY[setting_key], {}), "").strip(),
    }
    if target == "embedding":
        # `applicable` narrows what is OFFERED without narrowing what is SHOWN. The measured table
        # is evidence -- a deployment on Voyage is still entitled to see what self-hosting would
        # score -- but offering it five models it cannot serve, and none it can, is the defect.
        provider = os.environ.get("MATRIXARK_EMBEDDING_PROVIDER", "")
        applies = _encoder_applies(provider)
        for row in body["catalogue"]:
            row["applicable"] = applies
        body["provider_models"] = _hosted_encoders_for(provider)
    return body


def _shared_budget_summary() -> Json:
    """What each section of a pack is allowed, asked of the packer rather than worked out here.

    The field this replaces reported `MATRIXARK_MAX_BUDGET_TOKENS`, a variable NOTHING reads -- not
    the packer, not the engine, not the config file. It said 8192 while the section it named was
    deciding its size from a percentage of a 500,000-token budget, so the one number a customer
    could see about their pack was invented.

    `bound_by` is the point of the whole panel: a percentage that something else is overriding is
    a control that does not control anything, and that is exactly the state this deployment shipped
    in -- twice over. A ceiling in tokens could override it, and so could a guard on the share
    itself, which sat at exactly the share's own default and so refused every raise. Three limits
    can decide a section's size and the customer is owed the name of the one that did.
    """
    try:
        try:
            from tools import matrixark_mcp_budget_policies as policies  # type: ignore
            from tools import matrixark_mcp_runtime_config as runtime  # type: ignore
        except ImportError:
            import matrixark_mcp_budget_policies as policies  # type: ignore
            import matrixark_mcp_runtime_config as runtime  # type: ignore
    except Exception:  # pragma: no cover - portal still works without the retrieval modules
        return {"available": False}
    total = runtime.DEFAULT_MAX_CONTEXT_TOKENS
    policy = policies.build_shared_context_policy({}, {}, remote_budget_tokens=total)

    def section(prefix: str, variable: str, constant: str) -> Json:
        ratio = float(policy.get("%s_budget_ratio" % prefix) or 0.0)
        guard = float(policy.get("%s_max_budget_ratio" % prefix) or 0.0)
        ceiling = int(policy.get("%s_max_budget_tokens" % prefix) or 0)
        allowed = int(policy.get("%s_budget_tokens" % prefix) or 0)
        by_percentage = int(total * ratio)
        # What the deployment ASKED for, before the guard had its say. The packer only returns the
        # resolved share, so a raise the guard refused is invisible in the policy it hands back --
        # which is precisely the case a customer needs to see, and the reason this asks the
        # resolver rather than reading the resolved number twice.
        asked = runtime.live_float(variable, getattr(runtime, constant))
        if asked > ratio + 1e-9:
            bound_by = "share_guard"
        elif ceiling and by_percentage > ceiling:
            bound_by = "ceiling"
        else:
            bound_by = "percentage"
        return {
            "percent": round(ratio * 100, 2),
            "asked_percent": round(asked * 100, 2),
            "guard_percent": round(guard * 100, 2),
            "tokens": allowed,
            "ceiling_tokens": ceiling,
            "by_percentage_tokens": by_percentage,
            "bound_by": bound_by,
        }

    skills = section("skill", "MATRIXARK_SHARED_SKILL_BUDGET_RATIO",
                     "DEFAULT_SHARED_SKILL_BUDGET_RATIO")
    resources = section("resource", "MATRIXARK_SHARED_RESOURCE_BUDGET_RATIO",
                        "DEFAULT_SHARED_RESOURCE_BUDGET_RATIO")
    # The agent hooks do not use the budget above and never did. Reporting one number as "the"
    # context budget made every token figure on this panel wrong for the path that serves agents --
    # by fifty times on a deployment installed by the manual, which sets the hook budget to 10,000.
    # The SHARE is the same on both paths; only what it comes to differs, so both are reported and
    # neither is called the answer.
    hook_total = runtime.hook_max_context_tokens()
    paths = []
    for name, label, budget, variable in (
            ("api", "API callers", total, "MATRIXARK_DEFAULT_MAX_CONTEXT_TOKENS"),
            ("agent_hooks", "Agent hooks", hook_total, "MATRIXARK_HOOK_MAX_CONTEXT_TOKENS")):
        paths.append({
            "path": name,
            "label": label,
            "context_budget_tokens": budget,
            "variable": variable,
            "sections": {
                "skills": min(int(budget * float(policy.get("skill_budget_ratio") or 0.0)),
                              int(policy.get("skill_max_budget_tokens") or budget) or budget),
                "resources": min(
                    int(budget * float(policy.get("resource_budget_ratio") or 0.0)),
                    int(policy.get("resource_max_budget_tokens") or budget) or budget),
            },
        })
    return {
        "available": True,
        "context_budget_tokens": total,
        "skills": skills,
        "resources": resources,
        "paths": paths,
        # Not a warning: on a default deployment they DO differ, and a warning that always fires is
        # noise. It is a fact the reader needs in order to read the rows above correctly.
        "paths_differ": hook_total != total,
    }


def _model_config_snapshot() -> Json:
    """The effective extraction/embedding/skill-budget configuration, redacted for display.

    Operators need to see which model endpoints a deployment is actually talking to without shell
    access to the process environment. API keys are NEVER included: a key is configured by naming
    the environment variable that holds it (`MATRIXARK_EXTRACTION_API_KEY_ENV`), so this reports the
    variable's NAME and whether it currently resolves to a non-empty value -- never the value.

    `warnings` calls out the silent-degradation cases. Both extraction and embedding fall back to a
    deterministic local path when no provider is configured; that path answers 200 with hash vectors
    and rule-extracted memories, which is indistinguishable from a healthy system at the API
    surface. Surfacing it is the difference between a misconfigured deployment that looks fine and
    one an operator can see is misconfigured.
    """

    def _env(name: str, default: str = "") -> str:
        return os.environ.get(name, default).strip()

    def _key_state(env_name: str) -> Json:
        return {
            "api_key_env": env_name,
            "api_key_configured": bool(os.environ.get(env_name, "").strip()),
        }

    extraction_provider = _env(
        "MATRIXARK_UNDERSTANDING_PROVIDER", _env("MATRIXARK_EXTRACTION_PROVIDER", "deterministic")
    )
    embedding_provider = _env("MATRIXARK_EMBEDDING_PROVIDER", "deterministic")
    # Which variable a key lands in is the registry's decision, and asking it is the only way this
    # page can be right about it. Recomputing the name here is what let the portal report
    # OPENAI_API_KEY -- flat, for every provider -- while a Voyage encoder read VOYAGE_API_KEY and
    # an Anthropic extraction read ANTHROPIC_API_KEY. The warnings below tell a customer where to
    # put their key, so a second answer here is worse than none.
    extraction_key_env = _gwconfig._env_name(_gwconfig.SETTINGS_BY_KEY["extraction.api_key"], {})
    embedding_key_env = _gwconfig._env_name(_gwconfig.SETTINGS_BY_KEY["embedding.api_key"], {})
    require_model_embeddings = _env("MATRIXARK_REQUIRE_MODEL_EMBEDDINGS") in {"1", "true", "yes", "on"}

    extraction: Json = {
        "provider": extraction_provider,
        "base_url": _env("MATRIXARK_EXTRACTION_BASE_URL"),
        # The model the SELECTED provider reads, not one variable's value: Anthropic reads its
        # own, so a panel hardcoding the OpenAI one showed a field the deployment never sends.
        "model": _env(_gwconfig._env_name(
            _gwconfig.SETTINGS_BY_KEY["extraction.model"], {})),
        "timeout_sec": _env("MATRIXARK_EXTRACTION_TIMEOUT_SEC", "30"),
        "max_tokens": _env("MATRIXARK_EXTRACTION_MAX_TOKENS"),
        **_key_state(extraction_key_env),
    }
    embedding: Json = {
        "provider": embedding_provider,
        "api_base": _env("MATRIXARK_EMBEDDING_API_BASE") or _env("MATRIXARK_EMBED_BASE_URL"),
        "model": _env("MATRIXARK_EMBEDDING_MODEL"),
        "model_path": _env("MATRIXARK_EMBEDDING_MODEL_PATH"),
        "text_max_tokens": _env("MATRIXARK_EMBEDDING_TEXT_MAX_TOKENS", "128"),
        "require_model_embeddings": require_model_embeddings,
        **_key_state(embedding_key_env),
    }
    # `max_budget_tokens` used to sit here, reporting a variable nothing reads. What a customer
    # needs is what each section is actually allowed, which is a percentage of the context budget
    # until a ceiling says otherwise -- so the packer is asked, and both numbers are shown.
    budgets = _shared_budget_summary()
    skills: Json = {
        "shared_skill_budget_ratio": _env("MATRIXARK_SHARED_SKILL_BUDGET_RATIO", "0.10"),
        "budgets": budgets,
        "skill_chunks_per_skill": _env("MATRIXARK_SKILL_CHUNKS_PER_SKILL", "3"),
        "skill_reserved_refs": _env("MATRIXARK_SKILL_RESERVED_REFS", "3"),
    }

    warnings: List[str] = []
    # First, because it is the one that matters most and the only one about who may ask rather than
    # about what the answer is worth. `is False` on purpose: a process that never recorded a posture
    # does not know, and "we did not check" must not read as "you are safe".
    if _AUTH_POSTURE.get("require_auth") is False:
        warnings.append(
            "This gateway accepts anonymous requests: anyone who can reach this address has full "
            "access to every tenant's data, and there is no tenant isolation. That is the "
            "out-of-the-box default so the API works with no configuration. Set "
            "MATRIXARK_REQUIRE_AUTH=1 and MATRIXARK_ACCESS_MODE=enforced, then restart."
        )
    _retired_summary_model = _env("MATRIXARK_SUMMARY_MODEL")
    if _retired_summary_model:
        warnings.append(
            "MATRIXARK_SUMMARY_MODEL is set to " + _retired_summary_model + " and is no longer "
            "read: node summaries are made by the extraction endpoint with the extraction key, so "
            "they use Extraction model. Clear the variable, or set Extraction model to the one you "
            "want.")
    def _unrecognised(group: str, provider: str) -> str:
        """The warning for a value nothing matches. It names the value, because the whole failure is
        that the value looks configured -- and lists what would have worked."""
        return (
            "The " + group + " provider is set to " + repr(provider) + ", which nothing recognises. "
            "It is not rejected: the request falls through to the same local path as "
            "'deterministic', so the deployment answers 200 and looks configured. Set it to one of "
            + ", ".join(_gwconfig.recognised_providers(group)) + ".")

    unrecognised_extraction = _gwconfig.provider_is_unrecognised("extraction", extraction_provider)
    unrecognised_embedding = _gwconfig.provider_is_unrecognised("embedding", embedding_provider)
    if unrecognised_extraction:
        warnings.append(_unrecognised("extraction", extraction_provider))
    if unrecognised_embedding:
        warnings.append(_unrecognised("embedding", embedding_provider))
    if _gwconfig.extraction_provider_effect(extraction_provider) == "rules" \
            and not unrecognised_extraction:
        warnings.append(
            "Extraction provider is deterministic: no model is called, so ingest stores only "
            "what the local rules extract. Set Extraction provider to openai_compatible, then fill "
            "in Extraction base URL, Extraction model and Extraction API key below."
        )
    elif not extraction["api_key_configured"]:
        warnings.append(
            "Extraction API key is empty and Extraction provider is "
            + repr(extraction_provider) + ": extraction calls will fail and fall back to the "
            "deterministic path. The key goes into " + extraction_key_env + ", which is what "
            "Extraction key variable names."
        )
    for _name in ("skills", "resources"):
        _section = (budgets.get(_name) or {}) if budgets.get("available") else {}
        if _section.get("bound_by") == "ceiling":
            warnings.append(
                "The " + _name + " share of a pack is set to " + str(_section["percent"]) + "% of "
                + str(budgets["context_budget_tokens"]) + " tokens, which is "
                + str(_section["by_percentage_tokens"]) + ", but the ceiling beside it allows "
                + str(_section["ceiling_tokens"]) + ". The percentage is not what decides here: "
                "raise the ceiling, or set the percentage to what you actually want.")
    if _gwconfig.embedding_provider_effect(embedding_provider) == "hash" \
            and not unrecognised_embedding:
        warnings.append(
            "Embedding provider is deterministic: retrieval uses hash vectors, not semantic "
            "embeddings. Set Embedding provider to the encoder you run, and turn on Fail instead "
            "of falling back so an unreachable one is not answered with hash vectors."
        )
    else:
        if not require_model_embeddings and not unrecognised_embedding:
            warnings.append(
                "Fail instead of falling back is off: if the encoder becomes unreachable the "
                "gateway answers with hash vectors instead of failing the request, and retrieval "
                "looks healthy while it stops being semantic."
            )
        # Two configuration mistakes silently degrade an OpenAI-compatible encoder to hash vectors,
        # and neither is visible from the outside: the request 200s and retrieval still returns
        # plausible results. Both are cheap to check here.
        api_base = embedding["api_base"]
        if api_base and not api_base.rstrip("/").endswith("/v1"):
            warnings.append(
                "Embedding base URL (" + api_base + ") does not end in /v1: the endpoint is "
                "built as <base>/embeddings, so an OpenAI-compatible encoder serving "
                "/v1/embeddings is never reached and every vector is a hash fallback."
            )
        overriding_path = _env("MATRIXARK_EMBEDDING_MODEL_PATH")
        if overriding_path:
            if _gwconfig.embedding_provider_effect(embedding_provider) == "local_model":
                warnings.append(
                    "MATRIXARK_EMBEDDING_MODEL_PATH is set to " + overriding_path + ", and an "
                    "in-process encoder loads it in preference to Embedding model: the model named "
                    "above is not the one making vectors. Clear the variable, or put the path in "
                    "Embedding model, which accepts one.")
            else:
                warnings.append(
                    "MATRIXARK_EMBEDDING_MODEL_PATH is set to " + overriding_path + " and this "
                    "provider never reads it -- a hosted encoder is sent the model NAME. It is "
                    "doing nothing here, and would take over if the provider changed to an "
                    "in-process encoder.")
        if not embedding["api_key_configured"]:
            warnings.append(
                "Embedding API key is empty: the embedding call is skipped before it is "
                "attempted, even for a local encoder that needs no auth, so set it to any "
                "non-empty placeholder for a local endpoint. The key goes into "
                + embedding_key_env + ", which is what Embedding key variable names."
            )

    # Both key controls can name the SAME variable -- they both default to OPENAI_API_KEY -- and the
    # portal reports each secret as configured because each is STORED, while the variable holds
    # whichever was written last. Two providers on two endpoints then share one key, one of them is
    # authenticating with the other's, and the 401 falls back silently to the deterministic path.
    #
    # Only when the endpoints differ. One provider serving both sides is the `openai` preset, where
    # sharing the key is the point, and warning there would be noise on a correct configuration.
    extraction_endpoint = str(extraction["base_url"] or "").rstrip("/")
    embedding_endpoint = str(embedding["api_base"] or "").rstrip("/")
    if (_gwconfig.extraction_provider_effect(extraction_provider) != "rules"
            and _gwconfig.embedding_provider_effect(embedding_provider) == "api"
            and extraction_key_env and extraction_key_env == embedding_key_env
            and extraction_endpoint and embedding_endpoint
            and extraction_endpoint != embedding_endpoint):
        warnings.append(
            "Extraction API key and Embedding API key both go into " + extraction_key_env
            + ", but they call different endpoints (" + extraction_endpoint + " and "
            + embedding_endpoint + "): whichever was saved last is the one in that variable, so one "
            "of the two is authenticating with the other's key. Give one of them its own place in "
            "Extraction key variable or Embedding key variable, then set that key again."
        )

    return {
        "status": "ok",
        "extraction": extraction,
        "embedding": embedding,
        "skills": skills,
        "warnings": warnings,
        "encoders": encoder_catalog(),
        # Listed from here rather than hard-coded into the page, so an asset added to the registry
        # shows up on the portal without a matching edit to the page it is rendered on.
        "monitoring": monitoring_catalogue(_env("MATRIXARK_DATANODE_BLOB_URL")
                                           or _env("MATRIXARK_DATANODE_URL")
                                           or _DEFAULTS["datanode_url"]),
    }


# Every scope the backend gates on, in the order a customer meets them, with what it actually
# permits. The set is verified against MATRIXARK_TOOL_SCOPES by
# test_matrixark_gateway_portal.ScopeCatalogTest -- a scope the backend enforces and this does not
# describe would be one a customer cannot discover, and a scope described here that the backend
# does not know would be one they could grant to no effect.
SCOPE_CATALOG: List[Json] = [
    {"scope": "context:ingest", "label": "Write memories",
     "detail": "Ingest turns and documents, commit a session, and supersede a memory."},
    {"scope": "context:retrieve", "label": "Read memories",
     "detail": "Retrieve context packs, list memories and subjects, read one memory and its "
               "history, and read a resource's stored text."},
    {"scope": "context:forget", "label": "Delete memories",
     "detail": "Forget, delete and reset. Destructive; a serving agent does not need it."},
    {"scope": "context:feedback", "label": "Rate memories",
     "detail": "Record whether a retrieved memory was useful."},
    {"scope": "context:replay", "label": "Replay",
     "detail": "Re-run a stored request for debugging, and read the ingestion dashboard."},
    {"scope": "skill:read", "label": "List skills",
     "detail": "See which skills this scope can draw on."},
    {"scope": "skill:manage", "label": "Manage skills",
     "detail": "Enable, disable and re-tag a skill in the registry."},
    {"scope": "resource:read", "label": "List resources",
     "detail": "See which documents are stored."},
    {"scope": "portal:read", "label": "Portal",
     "detail": "Read the management portal payload."},
    {"scope": "admin:account", "label": "Manage accounts",
     "detail": "Create and list accounts and tenants."},
    {"scope": "admin:user", "label": "Manage users",
     "detail": "Create users and link external identities."},
    {"scope": "admin:sso", "label": "Map SSO identities",
     "detail": "Link a verified Google, GitHub, Okta or Azure AD subject to a MatrixArk user."},
    {"scope": "admin:api_key", "label": "Manage keys",
     "detail": "Create, rotate and revoke API keys, and read per-key usage. This is the scope the "
               "portal's own admin actions need."},
    {"scope": "admin:audit", "label": "Read the audit log",
     "detail": "Read admin activity, and per-key usage. Reading only: changing configuration or "
               "starting an import needs admin:api_key."},
]

# Four shapes that cover almost every key anyone actually issues. Named for the job rather than the
# permission, because the question a customer is answering is "what is this key for".
SCOPE_PRESETS: List[Json] = [
    {"id": "agent", "label": "Agent key",
     "detail": "A serving agent: writes what it learns, reads context back, rates what helped. "
               "Cannot delete, and cannot manage anything.",
     "scopes": ["context:ingest", "context:retrieve", "context:feedback",
                "skill:read", "resource:read"]},
    {"id": "read_only", "label": "Read-only key",
     "detail": "Dashboards, evaluation harnesses, anything that must not write.",
     "scopes": ["context:retrieve", "skill:read", "resource:read"]},
    {"id": "ingest", "label": "Ingest-only key",
     "detail": "A one-way pipe for a loader or a hook: writes, and cannot read another "
               "workload's memory back.",
     "scopes": ["context:ingest"]},
    {"id": "admin", "label": "Admin key",
     "detail": "Runs this portal: configuration, keys, usage and the audit log. Issue as few as "
               "you can.",
     "scopes": ["admin:api_key", "admin:audit", "admin:account", "admin:user", "portal:read"]},
]


# ================================================================================================
# The API surface, described
# ================================================================================================
# Served at GET /v1/admin/routes and rendered on the portal's API page, so the contract a customer
# reads is the one this process actually serves. test_matrixark_gateway_routes compares this list
# against the path literals in this file, in both directions: an undocumented route is one a
# customer cannot find, and a documented route that no longer exists is worse -- they will write
# against it.
#
# `body` is a request that works as written, so the page can offer a curl that runs rather than a
# schema to interpret.
# ================================================================================================
# The mem0 API, as operations rather than routes
# ================================================================================================
# A customer arrives holding mem0 code: `add`, `search`, `get_all`, `update`, `delete_all`. The
# route list answers a different question -- it is organised by URL -- so matching a method to a
# request means reading every entry and inferring. This table is the mapping, and it is the source
# the portal's console builds its forms from, so a method that gains an argument gains a field.
#
# Every entry names a path that appears in ROUTE_DOCS; a test asserts both directions, because an
# operation pointing at a route that does not exist is a console button that 404s, and a memory
# route no operation reaches is a capability nothing in the portal can exercise.
#
# Field placement, by `in`:
#   body     a top-level key of the JSON body
#   scope    a key inside the body's `scope` object
#   query    a query-string parameter
#   path     substituted into {id} in the path
#   message  the text of a single user turn -- the shape /v1/ingest wants
# A batch arrives in the request body, so these two are the only things standing between a paste
# and a worker thread holding it all. 8 MB and 20,000 records are both well above a plausible paste
# and well below anything that would hurt; past either, a directory import is the right tool.
MAX_BATCH_RECORDS_BYTES = 8 << 20
MAX_BATCH_RECORDS = 20_000

MEM0_OPERATIONS: List[Json] = [
    {"id": "add", "label": "add()", "group": "Write", "method": "POST", "path": "/v1/ingest",
     "scope": "context:ingest", "destructive": False, "needs_scope": True,
     "summary": "Write a turn. Acknowledged at 202 with extraction running behind it, which is "
                "why a search issued immediately afterwards can legitimately not see it yet.",
     "fields": [
         {"name": "content", "in": "message", "kind": "textarea", "required": True,
          "label": "Message",
          "placeholder": "The team agreed to keep the coupon stacking rule for ACME until Q4.",
          "help": "Sent as one user turn."},
         {"name": "finalize", "in": "body", "kind": "bool", "default": True,
          "label": "Extract before answering",
          "help": "Off is how production behaves: the write is acknowledged and extraction runs "
                  "in the background."},
         {"name": "metadata", "in": "body", "kind": "json", "label": "Metadata",
          "placeholder": '{"source": "portal"}',
          "help": "Stored alongside the memory and returned with it."},
     ]},
    {"id": "search", "label": "search()", "group": "Read", "method": "POST",
     "path": "/v1/retrieve", "scope": "context:retrieve", "destructive": False,
     "needs_scope": True,
     "summary": "The retrieval path an agent uses: ranked, then packed to fit a token budget.",
     "fields": [
         {"name": "query", "in": "body", "kind": "text", "required": True, "label": "Query",
          "placeholder": "when do we ship?"},
         {"name": "max_budget_tokens", "in": "body", "kind": "number", "default": 2048,
          "label": "Token budget",
          "help": "The pack is built to fit this. A budget far below what the answer needs is "
                  "indistinguishable from poor retrieval."},
     ]},
    {"id": "get_all", "label": "get_all()", "group": "Read", "method": "POST",
     "path": "/v1/memories", "scope": "context:retrieve", "destructive": False,
     "needs_scope": True,
     "summary": "Every live memory in the scope, unranked. This is the listing, not the search.",
     "fields": [
         {"name": "limit", "in": "body", "kind": "number", "default": 50, "label": "Limit"},
     ]},
    {"id": "get", "label": "get()", "group": "Read", "method": "GET", "path": "/v1/memory/{id}",
     "scope": "context:retrieve", "destructive": False, "needs_scope": False,
     "summary": "One memory's stored record, by id.",
     "fields": [
         {"name": "id", "in": "path", "kind": "text", "required": True, "label": "Memory id",
          "placeholder": "mem_7f21"},
     ]},
    {"id": "history", "label": "history()", "group": "Read", "method": "GET",
     "path": "/v1/memory/{id}/history", "scope": "context:retrieve", "destructive": False,
     "needs_scope": False,
     "summary": "How that memory changed: each supersede, with what it said before.",
     "fields": [
         {"name": "id", "in": "path", "kind": "text", "required": True, "label": "Memory id",
          "placeholder": "mem_7f21"},
     ]},
    {"id": "get_by_key", "label": "by identity key", "group": "Read", "method": "GET",
     "path": "/v1/memory/by-key", "scope": "context:retrieve", "destructive": False,
     "needs_scope": False,
     "summary": "The one live value for an identity key in a scope — the shape a profile field "
                "wants, where the answer is a value and not a ranked list.",
     "fields": [
         {"name": "identity_key", "in": "query", "kind": "text", "required": True,
          "label": "Identity key", "placeholder": "ship_date"},
         {"name": "user_id", "in": "query", "kind": "text", "label": "User", "from_scope": "user"},
     ]},
    {"id": "users", "label": "users()", "group": "Read", "method": "POST", "path": "/v1/users",
     "scope": "context:retrieve", "destructive": False, "needs_scope": True,
     "summary": "Which users, agents and runs hold memories in this tenant.",
     "fields": []},
    {"id": "update", "label": "update()", "group": "Write", "method": "POST", "path": "/v1/update",
     "scope": "context:ingest", "destructive": False, "needs_scope": False,
     "summary": "Supersede a memory: the amended text is ingested and the old id tombstoned, so "
                "history keeps both.",
     "fields": [
         {"name": "memory_id", "in": "body", "kind": "text", "required": True,
          "label": "Memory id", "placeholder": "mem_7f21"},
         {"name": "text", "in": "body", "kind": "textarea", "required": True, "label": "New text",
          "placeholder": "We ship on Friday."},
     ]},
    {"id": "feedback", "label": "feedback()", "group": "Write", "method": "POST",
     "path": "/v1/memory/feedback", "scope": "context:ingest", "destructive": False,
     "needs_scope": False,
     "summary": "Rate a retrieved memory. A write about a memory, so it gates like a write.",
     "fields": [
         {"name": "memory_id", "in": "body", "kind": "text", "required": True,
          "label": "Memory id", "placeholder": "mem_7f21"},
         {"name": "rating", "in": "body", "kind": "number", "default": 1, "label": "Rating",
          "help": "1 useful, -1 not."},
     ]},
    {"id": "session_commit", "label": "commit a session", "group": "Write", "method": "POST",
     "path": "/v1/session/commit", "scope": "context:ingest", "destructive": False,
     "needs_scope": True,
     "summary": "Close a session and roll its turns into summaries. Until this runs the turns are "
                "stored but not summarised.",
     "fields": []},
    {"id": "forget", "label": "forget()", "group": "Forget", "method": "POST", "path": "/v1/forget",
     "scope": "context:forget", "destructive": True, "needs_scope": False,
     "summary": "Stop returning one memory. The record stays, so history still explains it.",
     "fields": [
         {"name": "memory_id", "in": "body", "kind": "text", "required": True,
          "label": "Memory id", "placeholder": "mem_7f21"},
     ]},
    {"id": "delete", "label": "delete()", "group": "Forget", "method": "POST", "path": "/v1/delete",
     "scope": "context:forget", "destructive": True, "needs_scope": False,
     "summary": "Delete one memory outright.",
     "fields": [
         {"name": "memory_id", "in": "body", "kind": "text", "required": True,
          "label": "Memory id", "placeholder": "mem_7f21"},
     ]},
    {"id": "delete_all", "label": "delete_all()", "group": "Forget", "method": "POST",
     "path": "/v1/reset", "scope": "context:forget", "destructive": True, "needs_scope": True,
     "summary": "Drop everything in the scope shown above. There is no undo, and an empty user "
                "field means the default scope, not none of them.",
     "fields": []},
]

ROUTE_DOCS: List[Json] = [
    # ---- health -------------------------------------------------------------------------------
    {"group": "Health", "method": "GET", "path": "/v1/healthz", "scope": None,
     "summary": "Liveness. Answers as long as the process is up."},
    {"group": "Health", "method": "GET", "path": "/v1/readyz", "scope": None,
     "summary": "Readiness. 200 only when the datanode can serve; 503 with "
                "\"datanode\": \"erroring\" or \"unreachable\" when it cannot, so a gateway "
                "with a backend that is down takes itself out of rotation."},
    {"group": "Health", "method": "GET", "path": "/v1/metrics", "scope": None,
     "summary": "Prometheus scrape. Aggregate counters only, so it needs no credentials."},

    # ---- memory -------------------------------------------------------------------------------
    {"group": "Memory", "method": "POST", "path": "/v1/ingest", "scope": "context:ingest",
     "summary": "Write turns or records. Fast-acks 202; extraction runs behind it unless you "
                "pass finalize.",
     "body": {"scope": {"user_id": "alice"},
              "messages": [{"role": "user", "content": "We ship on Thursday."}]}},
    {"group": "Memory", "method": "POST", "path": "/v1/ingest_file", "scope": "context:ingest",
     "summary": "Stream a file to the blob tier and ingest it in one call. Headers: X-Filename, "
                "X-Resource-Kind, X-Resource-Type, X-Scope (a JSON scope object), X-Wait, "
                "X-Sharing-Scope. The body is the raw file.",
     "raw_body": True},
    {"group": "Memory", "method": "POST", "path": "/v1/session/commit", "scope": "context:ingest",
     "summary": "Close a session and roll its turns into summaries.",
     "body": {"scope": {"user_id": "alice", "session_id": "s-42"}}},
    {"group": "Memory", "method": "POST", "path": "/v1/retrieve", "scope": "context:retrieve",
     "summary": "Build a context pack for a query, inside a token budget.",
     "body": {"scope": {"user_id": "alice"}, "query": "when do we ship?",
              "max_budget_tokens": 2048}},
    {"group": "Memory", "method": "GET", "path": "/v1/memories", "scope": "context:retrieve",
     "summary": "List a scope's live memories. Query: user_id, agent_id, session_id, limit.",
     "query": "user_id=alice&limit=50"},
    {"group": "Memory", "method": "POST", "path": "/v1/memories", "scope": "context:retrieve",
     "summary": "The same listing with a JSON body.",
     "body": {"scope": {"user_id": "alice"}, "limit": 50}},
    {"group": "Memory", "method": "GET", "path": "/v1/memory/{id}", "scope": "context:retrieve",
     "summary": "One memory's stored record."},
    {"group": "Memory", "method": "GET", "path": "/v1/memory/{id}/history",
     "scope": "context:retrieve", "summary": "How that memory changed over time."},
    {"group": "Memory", "method": "GET", "path": "/v1/memory/by-key", "scope": "context:retrieve",
     "summary": "The single live value for an identity_key in a scope. identity_key is required.",
     "query": "identity_key=ship_date&user_id=alice"},
    {"group": "Memory", "method": "POST", "path": "/v1/memory/feedback", "scope": "context:ingest",
     "summary": "Rate a retrieved memory. A write about a memory, so it gates like a write.",
     "body": {"memory_id": "mem_7f21", "rating": 1}},
    {"group": "Memory", "method": "POST", "path": "/v1/update", "scope": "context:ingest",
     "summary": "Supersede a memory: ingest the amended text and tombstone the old id.",
     "body": {"memory_id": "mem_7f21", "text": "We ship on Friday."}},
    {"group": "Memory", "method": "POST", "path": "/v1/forget", "scope": "context:forget",
     "summary": "Forget one memory.", "body": {"memory_id": "mem_7f21"}},
    {"group": "Memory", "method": "POST", "path": "/v1/delete", "scope": "context:forget",
     "summary": "Delete a memory outright.", "body": {"memory_id": "mem_7f21"}},
    {"group": "Memory", "method": "POST", "path": "/v1/reset", "scope": "context:forget",
     "summary": "Drop everything in a scope. Destructive; there is no undo.",
     "body": {"scope": {"user_id": "alice"}}},
    {"group": "Memory", "method": "GET", "path": "/v1/users", "scope": "context:retrieve",
     "summary": "Which users, agents and runs hold memories. Query: user_id, agent_id, "
                "session_id, limit.",
     "query": "limit=50"},
    {"group": "Memory", "method": "POST", "path": "/v1/users", "scope": "context:retrieve",
     "summary": "The same listing with a JSON body.", "body": {"scope": {}}},

    # ---- catalogue ----------------------------------------------------------------------------
    {"group": "Catalogue", "method": "GET", "path": "/v1/skills", "scope": "skill:read",
     "summary": "Skills visible to a scope. Query: user_id, agent_id, session_id, limit "
                "(clamped at 500), include_disabled.",
     "query": "user_id=alice&limit=100"},
    {"group": "Catalogue", "method": "GET", "path": "/v1/resources", "scope": "resource:read",
     "summary": "Resources visible to a scope. Query: as above, plus resource_type.",
     "query": "user_id=alice&resource_type=md"},
    {"group": "Catalogue", "method": "POST", "path": "/v1/resource/content",
     "scope": "context:retrieve",
     "summary": "One resource's or skill's stored text, a page at a time.",
     "body": {"resource_hash": 901, "chunk_offset": 0, "chunk_limit": 20}},
    {"group": "Catalogue", "method": "POST", "path": "/v1/skills/update", "scope": "skill:manage",
     "summary": "Enable, disable or re-tag a skill without rewriting its manifest.",
     "body": {"skill_hash": 111, "status": "disabled"}},

    # ---- blobs and MCP ------------------------------------------------------------------------
    {"group": "Blobs", "method": "PUT", "path": "/v1/blob/{key}", "scope": "context:ingest",
     "summary": "Stream bytes to the blob tier, tenant-isolated. POST is accepted as an alias.",
     "raw_body": True},
    {"group": "Blobs", "method": "GET", "path": "/v1/blob/{key}", "scope": "context:retrieve",
     "summary": "Stream those bytes back."},
    {"group": "Blobs", "method": "POST", "path": "/v1/mcp", "scope": "per tool",
     "summary": "MCP over HTTP. Gated per tool from the body, not at the route.",
     "body": {"jsonrpc": "2.0", "id": 1, "method": "tools/list"}},

    # ---- administration -----------------------------------------------------------------------
    {"group": "Administration", "method": "GET", "path": "/v1/admin/overview", "scope": "admin",
     "summary": "The readiness checklist, counts and traffic in one call."},
    {"group": "Administration", "method": "GET", "path": "/v1/admin/config", "scope": "admin",
     "summary": "Effective model configuration, warnings, the writable registry and the "
                "read-only deployment inventory."},
    {"group": "Administration", "method": "POST", "path": "/v1/admin/config", "scope": "admin",
     "summary": "Write settings. A null value resets one to its built-in default.",
     "body": {"settings": {"embedding.provider": "openai_compatible",
                           "embedding.api_base": "http://127.0.0.1:8400/v1"}}},
    {"group": "Administration", "method": "GET", "path": "/v1/admin/config/export",
     "scope": "admin",
     "summary": "The configuration as a patch body for another deployment. Secrets omitted.",
     "query": "include_defaults=0"},
    {"group": "Administration", "method": "POST", "path": "/v1/admin/config/preset",
     "scope": "admin", "summary": "Apply a provider preset.", "body": {"preset": "deepseek"}},
    {"group": "Administration", "method": "POST", "path": "/v1/admin/config/test", "scope": "admin",
     "summary": "Call the configured extraction and embedding endpoints and report what came back.",
     "body": {}},
    {"group": "Administration", "method": "GET", "path": "/v1/admin/policy", "scope": "admin",
     "summary": "One user's effective settings: every knob's value, which layer it came from "
                "(user, tenant, environment or default), and whether a user may set it. The "
                "tenant comes from the key. Query: user_id.",
     "query": "user_id=alice"},
    {"group": "Administration", "method": "POST", "path": "/v1/admin/policy", "scope": "admin",
     "summary": "Change settings for one user (level=user, the default) or for everyone in the "
                "tenant (level=tenant). Applies to the next request with no restart, and is "
                "written to the policy file when one is configured. At level=user, settings that "
                "decide what is WRITTEN into the shared store are refused and named in the "
                "response; at level=tenant they are accepted, because a tenant owns its store.",
     "body": {"level": "user", "user_id": "alice", "settings": {"top_k_per_layer": 24}}},
    {"group": "Administration", "method": "GET", "path": "/v1/admin/models", "scope": "admin",
     "summary": "Models to choose from: a curated catalogue, what the configured endpoint says it "
                "serves, and — for embeddings — what the stored vectors were actually made with. "
                "Query: target (extraction|embedding), probe (1 to ask the endpoint).",
     "query": "target=extraction&probe=0"},
    {"group": "Administration", "method": "GET", "path": "/v1/admin/monitoring/{asset}",
     "scope": "admin",
     "summary": "A monitoring asset as this build defines it: gateway or ingestion for a Grafana "
                "dashboard, alerts for the Prometheus rules. Served from the process so the panels "
                "match the metrics it emits."},
    {"group": "Administration", "method": "GET", "path": "/v1/admin/events", "scope": "admin",
     "summary": "Server-sent events carrying this deployment's live state: traffic, imports in "
                "progress, encoding backlog and the configuration-warning count. One stream in "
                "place of polling; the browser reconnects by itself.",
     "stream": True},
    {"group": "Administration", "method": "GET", "path": "/v1/admin/embeddings", "scope": "admin",
     "summary": "How much of the store is encoded and how much is still waiting, plus the models "
                "and vector widths in use. Query: user_id, agent_id, session_id.",
     "query": "user_id=alice"},
    {"group": "Administration", "method": "GET", "path": "/v1/admin/scopes", "scope": "admin",
     "summary": "What each scope permits, plus four ready-made key shapes."},
    {"group": "Administration", "method": "GET", "path": "/v1/admin/audit", "scope": "admin",
     "summary": "Recent audit records for this tenant, newest first, with the recording mode "
                "beside them. Requires a key carrying admin:audit. An empty list with recording "
                "\"off\" means nothing is being kept, not that nothing happened.",
     "query": "limit (1-500, default 100)"},
    {"group": "Administration", "method": "GET", "path": "/v1/admin/api_key_usage",
     "scope": "admin", "summary": "Per-key edge counters: totals, ingest/retrieve split, bytes, "
                                  "first and last use."},
    {"group": "Administration", "method": "GET", "path": "/v1/admin/ingestion/jobs",
     "scope": "admin", "summary": "Bulk import jobs on this worker."},
    {"group": "Administration", "method": "GET", "path": "/v1/admin/deployment",
     "scope": "admin",
     "summary": "The deployment shapes that can be launched -- one box, replicated with Raft, or "
                "nodes in front of shared storage -- with the storage each supports, plus the "
                "backend this deployment is running right now, read from the datanode rather than "
                "inferred from configuration."},
    {"group": "Administration", "method": "POST", "path": "/v1/admin/deployment/plan",
     "scope": "admin",
     "summary": "Compose a deployment and report what the engine will actually do with it. "
                "Several choices resolve to something other than what was asked with no error: "
                "shared storage with no directory, and MatrixObject on a build without it, both "
                "fall through to auto-detection. Returns the environment, an env file, what is "
                "blocked, and what will differ from the request. Changes nothing.",
     "body": {"shape": "raft", "storage": "ebs", "nodes": 3,
              "root": "/var/lib/temporalstore", "key_envs": ["DEEPSEEK_API_KEY"]}},
    {"group": "Administration", "method": "POST", "path": "/v1/admin/ingestion/jobs",
     "scope": "admin",
     "summary": "Start a bulk import. Every path resolves inside MATRIXARK_INGESTION_ROOT; pass "
                "preview to count what would be sent without importing it.",
     "body": {"directory": "/srv/playbooks", "globs": ["*.md"], "preview": True},
     "needs": "MATRIXARK_INGESTION_ROOT"},
    {"group": "Administration", "method": "POST", "path": "/v1/admin/ingestion/records",
     "scope": "admin",
     "summary": "Ingest a batch of memories as a background job. Each record needs text; "
                "user_id, agent_id, session_id, identity_key, role and metadata are optional and "
                "override the batch's. Pass preview to count and check without ingesting.",
     "body": {"records": [{"text": "We ship on Thursday.", "user_id": "alice"}],
              "user_id": "alice", "preview": True}},
    {"group": "Administration", "method": "POST",
     "path": "/v1/admin/ingestion/jobs/{id}/retry", "scope": "admin",
     "summary": "Re-run a finished job's failed documents as a new job, linked to it. By default "
                "only the failures worth retrying -- a timeout or an exhausted 5xx, not a 4xx. "
                "Pass only_retryable=false to resubmit everything that failed.",
     "body": {"only_retryable": True}},
    {"group": "Administration", "method": "POST",
     "path": "/v1/admin/ingestion/jobs/{id}/cancel", "scope": "admin",
     "summary": "Stop a job before its next document. What is imported stays imported; ingest is "
                "a keyed upsert, so re-running replaces rather than duplicates."},

    # ---- portal pages -------------------------------------------------------------------------
    {"group": "Portal pages", "method": "GET", "path": "/v1/admin", "scope": None,
     "summary": "Overview. Fetching any page needs nothing; every action on it is gated."},
    {"group": "Portal pages", "method": "GET", "path": "/v1/admin/setup", "scope": None,
     "summary": "Setup and metrics."},
    {"group": "Portal pages", "method": "GET", "path": "/v1/admin/catalog", "scope": None,
     "summary": "Skills and resources."},
    {"group": "Portal pages", "method": "GET", "path": "/v1/admin/explore", "scope": None,
     "summary": "Ask, add, upload, browse."},
    {"group": "Portal pages", "method": "GET", "path": "/v1/admin/ingestion", "scope": None,
     "summary": "Bulk import."},
    {"group": "Portal pages", "method": "GET", "path": "/v1/admin/portal", "scope": None,
     "summary": "API keys."},
    {"group": "Portal pages", "method": "GET", "path": "/v1/admin/api", "scope": None,
     "summary": "This list."},
    {"group": "Portal pages", "method": "GET", "path": "/v1/admin/routes", "scope": None,
     "summary": "This list as JSON."},
]


# How often a live frame is sent. Two seconds is a compromise: an import moving at tens of
# documents a second visibly advances, and a forgotten tab costs one small frame every two seconds
# rather than three HTTP requests.
# How many recent failures ride the live frame. The edge keeps more; this is what a panel
# shows before someone goes looking properly.
LIVE_FAILURES = 10

EVENT_TICK_S = 2.0
# The embedding count walks the record log, so it rides the stream at its own much slower cadence
# rather than every tick. Everything else in a frame is read from memory.
EVENT_EMBEDDING_REFRESH_S = 30.0
# While something is waiting to be encoded the number is the thing being watched, so it is read
# often enough to be seen moving. Answering it walks the record log, which is why the idle case is
# not this: for a store with nothing pending the figure is a constant, and re-deriving a constant
# every few seconds is a standing load on every connected browser.
EVENT_EMBEDDING_REFRESH_DRAINING_S = 4.0


def _embedding_refresh_interval(embedding: Optional[Json]) -> float:
    """How long before the encoding figures are worth reading again.

    Unknown counts as draining: if the backend could not be asked, the next answer matters sooner
    than in half a minute, and it is the case where a customer is most likely to be watching.
    """
    if not isinstance(embedding, dict):
        return EVENT_EMBEDDING_REFRESH_DRAINING_S
    waiting = int(embedding.get("pending") or 0) + int(embedding.get("deferred_tasks") or 0)
    return (EVENT_EMBEDDING_REFRESH_DRAINING_S if waiting
            else EVENT_EMBEDDING_REFRESH_S)
# A stream is closed after this long and the browser reconnects. Bounded on purpose: an abandoned
# tab should not hold a connection for the life of the worker, and a reconnect is one request.
EVENT_STREAM_MAX_S = 600.0


# ---- live frames, built once per tick for the deployment rather than once per viewer ------------
# Everything in a frame used to be built inside each connection's own loop, so a second browser tab
# cost a second copy of all of it, exactly linear to sixteen viewers when measured. What a frame
# carries is one deployment's state; the number of people looking at it is not part of the answer.
# How often the frame re-probes the datanode. Slower than the tick on purpose: this is the one
# part of a frame that costs an outbound connection, and a backend that just went down is worth
# knowing about within half a minute rather than within two seconds.
DATANODE_REFRESH_S = 30.0

_LIVE_DATANODE: Optional[tuple] = None
# (event loop, task) while a probe is running; everyone else waits on it.
_LIVE_DATANODE_INFLIGHT: Optional[tuple] = None

_LIVE_SHARED: Optional[Json] = None
_LIVE_SHARED_AT = 0.0
_LIVE_EMBEDDING: dict = {}
# identity -> (event loop, task). One backend read in flight per identity; the rest wait.
_LIVE_EMBEDDING_INFLIGHT: dict = {}


def _reset_live_cache() -> None:
    """Drop both caches. For tests, which need a frame to reflect what they just changed."""
    global _LIVE_SHARED, _LIVE_SHARED_AT
    _LIVE_SHARED = None
    _LIVE_SHARED_AT = 0.0
    global _LIVE_DATANODE, _LIVE_DATANODE_INFLIGHT
    _LIVE_DATANODE = None
    _LIVE_DATANODE_INFLIGHT = None
    _LIVE_EMBEDDING.clear()
    _LIVE_EMBEDDING_INFLIGHT.clear()


def _forget_idle_identities(now: float) -> None:
    """Drop live-cache entries that their own readers would already ignore.

    The cache is keyed on the identity triple, and nothing removed an entry. A worker's resident
    memory therefore grew with the number of distinct keys that had EVER opened a status stream
    rather than the number watching one -- measured at 2,433 bytes per identity across the two
    caches this once swept, held for the life of the process, of which every byte was already too
    stale to be served.

    The tests below are the readers' own staleness checks, called with the same arguments. An entry
    is dropped exactly when it had stopped being an answer, so nothing that would have been served
    to anyone is evicted, and a viewer that comes back simply misses the way it already did.

    Run from the once-per-tick rebuild, so the cost is one pass over the identities seen in the
    last tick rather than anything per connection. When the last viewer leaves, that tick's entries
    stay behind until somebody watches again -- bounded by the viewers of one tick, which is the
    thing that was unbounded before.
    """
    for identity, entry in list(_LIVE_EMBEDDING.items()):
        if (now - entry[0]) >= _embedding_refresh_interval(entry[1]):
            _LIVE_EMBEDDING.pop(identity, None)
    for identity, entry in list(_LIVE_EMBEDDING_INFLIGHT.items()):
        # A task belonging to a closed loop can never be awaited -- `_embedding_for` checks the loop
        # before waiting on one -- so this entry is unreachable rather than merely stale.
        if entry[0].is_closed():
            _LIVE_EMBEDDING_INFLIGHT.pop(identity, None)


def _shared_live_parts() -> Json:
    """Traffic, imports and the warning count: identical for every viewer, so built once.

    Synchronous on purpose. Nothing here awaits, so nothing can interleave between the staleness
    check and the fill, and no lock is needed to keep two viewers from building it twice.

    Held for one tick. That is the cadence the state is published at anyway, so a viewer whose tick
    lands just after a rebuild sees at most one tick of age -- the same age it would have seen from
    a frame built for it alone.
    """
    global _LIVE_SHARED, _LIVE_SHARED_AT
    now = time.time()
    if _LIVE_SHARED is not None and (now - _LIVE_SHARED_AT) < EVENT_TICK_S:
        return _LIVE_SHARED
    try:
        traffic = _gwmetrics.METRICS.snapshot()
    except Exception:
        traffic = {}
    try:
        imports = _import_progress()
    except Exception:
        imports = {}
    try:
        # 68.6% of a frame, to take one integer out of a redacted configuration document that
        # reads about thirty environment variables to build.
        warnings = len(_model_config_snapshot().get("warnings") or [])
    except Exception:
        warnings = 0
    try:
        # Settings written since this process started and still not in effect. Deployment-wide, so
        # it belongs on the shared half of the frame, and deliberately not read out of snapshot():
        # this walks the settings list rather than building that document again.
        waiting = len(_gwconfig.pending_restart_keys())
    except Exception:
        waiting = 0
    try:
        # When the stored configuration was last written, so an open portal can notice a change
        # made in another tab or by another operator instead of waiting for its own slow timer.
        # The fact and the time only -- no values; those stay behind the admin-gated read.
        changed_at = float(_gwconfig.load().get("updated_at") or 0.0)
    except Exception:
        # Absent rather than 0: never written and could not tell are different, and a page that
        # treated "could not tell" as a change would re-read on every tick.
        changed_at = None
    _LIVE_SHARED = {
        "traffic": {
            "total_requests": traffic.get("total_requests", 0),
            "total_errors": traffic.get("total_errors", 0),
            "in_flight": traffic.get("in_flight", 0),
            "routes": traffic.get("routes", {}),
            # The newest few, not all fifty. This rides every frame to every viewer; the whole ring
            # would roughly double a frame to carry a scroll-back nobody reads in a strip.
            "recent_failures": (traffic.get("recent_failures") or [])[:LIVE_FAILURES],
        },
        "imports": imports,
        "warnings": warnings,
        "settings_waiting": waiting,
        "config_changed_at": changed_at,
    }
    _LIVE_SHARED_AT = now
    # Once per tick for the deployment, in the branch that already runs once per tick, so the
    # per-identity caches are bounded by who is watching rather than by who ever watched.
    _forget_idle_identities(now)
    return _LIVE_SHARED


def _frame_signature(frame: Json) -> bytes:
    """What THIS frame says, ignoring when it said it.

    The timestamp is excluded deliberately. It changes every tick by definition, so a comparison
    that included it would never find two frames equal -- the check would run, cost something, and
    never once skip a send.

    Derived from the frame every time, and it takes no identity, because there is no answer here
    that depends on who is asking. It used to be cached per identity for a tick so that two viewers
    on one key did not each serialise the same answer. On a hit that returned the signature of a
    frame built EARLIER and ignored the one passed in -- and the caller sends only when the
    signature differs from the last it sent, so a viewer could be told nothing had changed while
    holding a frame that had, and waited another tick for state it already had.

    The cache saved 54 microseconds a call: 0.27 ms of CPU per second at ten viewers on one
    identity, 2.7 ms at a hundred. A status stream arriving a tick late is the thing it exists not
    to be.
    """
    return json.dumps({field: value for field, value in frame.items() if field != "ts"},
                      default=str, sort_keys=True).encode("utf-8")


async def _datanode_for_frame(cfg: GatewayConfig) -> Optional[str]:
    """The datanode's state for the frame: shared by every viewer, refreshed slowly.

    Deployment-wide, so it is cached globally rather than per identity -- whether the backend is up
    is the same answer for everyone, and it carries nothing about who asked.

    Not read from the readiness route's recorded value: that only moves when something calls
    /v1/readyz, so a deployment nobody probes would show a stale answer or none at all, and the
    strip should not depend on somebody else's health check being configured.
    """
    global _LIVE_DATANODE, _LIVE_DATANODE_INFLIGHT
    if _LIVE_DATANODE is not None and (time.time() - _LIVE_DATANODE[1]) < DATANODE_REFRESH_S:
        return _LIVE_DATANODE[0]

    # One probe in flight for the whole deployment. Caching the result alone leaves the cold start
    # uncollapsed -- viewers arrive together, find nothing cached, and each opens its own
    # connection to the same backend. That is the same stampede the embedding read has, and it is
    # worse here because this one is a network probe rather than a local read.
    loop = asyncio.get_event_loop()
    pending = _LIVE_DATANODE_INFLIGHT
    if pending is not None and pending[0] is loop and not pending[1].done():
        return await asyncio.shield(pending[1])

    async def _probe_and_cache() -> Optional[str]:
        global _LIVE_DATANODE
        try:
            state = await asyncio.to_thread(_probe_datanode, cfg)
        except Exception:
            # The frame is not the place to fail. Absent reads as "not known", which is true.
            return None if _LIVE_DATANODE is None else _LIVE_DATANODE[0]
        _LIVE_DATANODE = (state, time.time())
        # Keeps the gauge fresh too, so the series does not depend on an orchestrator polling
        # readiness.
        _gwmetrics.METRICS.note_datanode(state)
        return state

    task = loop.create_task(_probe_and_cache())
    _LIVE_DATANODE_INFLIGHT = (loop, task)
    try:
        return await asyncio.shield(task)
    finally:
        if _LIVE_DATANODE_INFLIGHT is not None and _LIVE_DATANODE_INFLIGHT[1] is task                 and task.done():
            _LIVE_DATANODE_INFLIGHT = None


async def _embedding_for(server: Any, cfg: GatewayConfig, key: Optional[str],
                         tenant: Optional[str], account: Optional[str]) -> Optional[Json]:
    """The embedding backlog for ONE identity, reused by that identity's other viewers.

    Keyed on the whole identity triple rather than on the tenant. The read applies identity to the
    backend call, so a coarser key would let one identity be served another's backlog -- the sort
    of sharing that is invisible until it is a disclosure.
    """
    identity = (key, tenant, account)
    cached = _LIVE_EMBEDDING.get(identity)
    if cached is not None:
        at, value = cached
        if (time.time() - at) < _embedding_refresh_interval(value):
            return value

    loop = asyncio.get_event_loop()
    pending = _LIVE_EMBEDDING_INFLIGHT.get(identity)
    if pending is not None:
        # Only if it belongs to THIS loop. A task outlives its loop, and awaiting one whose loop
        # has closed raises rather than returning an answer.
        pending_loop, pending_task = pending
        if pending_loop is loop and not pending_task.done():
            return await asyncio.shield(pending_task)

    async def _read_and_cache() -> Optional[Json]:
        # The task caches, not the awaiter: a viewer that disconnects mid-read still leaves the
        # answer behind for everyone else waiting on it.
        value = await _read_embedding(server, cfg, key, tenant, account)
        _LIVE_EMBEDDING[identity] = (time.time(), value)
        return value

    task = loop.create_task(_read_and_cache())
    _LIVE_EMBEDDING_INFLIGHT[identity] = (loop, task)
    try:
        # Shielded, so this viewer going away does not cancel the read the others are waiting on.
        return await asyncio.shield(task)
    finally:
        current = _LIVE_EMBEDDING_INFLIGHT.get(identity)
        if current is not None and current[1] is task and task.done():
            _LIVE_EMBEDDING_INFLIGHT.pop(identity, None)


async def _event_frame(server: Any, cfg: GatewayConfig, key: Optional[str],
                       tenant: Optional[str], account: Optional[str],
                       embedding: Optional[Json],
                       datanode: Optional[str] = None) -> Json:
    """One frame of live state.

    The deployment-wide parts come from `_shared_live_parts`, which builds them once per tick for
    everyone watching; `embedding` is passed in already resolved for this viewer's identity. The
    timestamp is this frame's, not the shared part's, so a viewer can still tell frames apart.
    """
    shared = _shared_live_parts()
    return {
        "ts": time.time(),
        "traffic": shared["traffic"],
        "imports": shared["imports"],
        "warnings": shared["warnings"],
        # Built once per tick beside the rest and, until now, left behind here -- so the strip's
        # "awaiting restart" segment had nothing to render and never appeared, on any deployment.
        "settings_waiting": shared.get("settings_waiting"),
        "config_changed_at": shared.get("config_changed_at"),
        "embedding": embedding,
        # Absent when nothing has looked yet. "not known" and "unreachable" are different answers
        # and the strip renders them differently.
        "datanode": datanode,
    }


async def _read_embedding(server: Any, cfg: GatewayConfig, key: Optional[str],
                          tenant: Optional[str], account: Optional[str]) -> Optional[Json]:
    args: Json = {"scope": {}}
    _apply_identity(args, key, tenant, account)
    try:
        result = await asyncio.wait_for(
            asyncio.to_thread(server.call_tool, "matrixark_embedding_status", args),
            cfg.backend_timeout)
    except Exception:
        # A backend that cannot answer must leave the field absent rather than reporting an empty
        # backlog: "nothing pending" and "I could not find out" are different answers.
        return None
    if not isinstance(result, dict):
        return None
    slim = {field: result.get(field) for field in
            ("total", "encoded", "pending", "percent_encoded", "mixed_dimensions",
             "deferred_tasks")}
    slim["encoder"] = _encoder_summary()
    return slim


async def _event_stream(server: Any, cfg: GatewayConfig, scope: Json, receive: Callable,
                        send: Callable, key: Optional[str], tenant: Optional[str],
                        account: Optional[str]) -> None:
    """Server-sent events: the live state of this deployment, pushed.

    Three pages were each polling three endpoints on their own timers. One stream carries the same
    state, the server builds it once, and the page stops guessing an interval -- an import that
    finishes between two polls used to leave a stale bar on screen until the next one.

    Server-sent events rather than a websocket because the traffic is one-way and EventSource
    reconnects by itself; there is no protocol here to get wrong.
    """
    await send({"type": "http.response.start", "status": 200, "headers": [
        (b"content-type", b"text/event-stream; charset=utf-8"),
        (b"cache-control", b"no-cache, no-store"),
        (b"connection", b"keep-alive"),
        # Nginx buffers a proxied response by default, which turns a live stream into a single
        # delivery when it finishes -- the one deployment detail that silently defeats SSE.
        (b"x-accel-buffering", b"no"),
    ]})

    disconnected = asyncio.Event()

    async def watch_for_disconnect() -> None:
        while True:
            message = await receive()
            if message.get("type") == "http.disconnect":
                disconnected.set()
                return

    watcher = asyncio.ensure_future(watch_for_disconnect())
    started = time.time()
    # Spread the age limit. A fixed ceiling means every stream opened together also ends together,
    # and the client is told to retry after a fixed 3s -- so the reconnects arrive as a herd, and
    # keep re-forming every ten minutes for as long as the tabs are open. A tenth either way is
    # enough to break that up without making the lifetime unpredictable to anyone reading it.
    max_age = EVENT_STREAM_MAX_S * (0.9 + random.random() * 0.2)
    embedding: Optional[Json] = None
    # Per connection, not shared: a viewer that has just arrived has nothing on screen, so its
    # first frame must always be sent, however long the deployment has been quiet.
    last_signature: Optional[bytes] = None

    async def emit(payload: bytes) -> None:
        await send({"type": "http.response.body", "body": payload, "more_body": True})

    try:
        # Tell the browser how long to wait before reconnecting, so a closed stream comes back on
        # our cadence rather than its default.
        await emit(b"retry: 3000\n\n")
        while not disconnected.is_set():
            # Per identity rather than per connection: two tabs open on the same key asked the
            # backend the same question twice on every refresh.
            embedding = await _embedding_for(server, cfg, key, tenant, account)
            datanode = await _datanode_for_frame(cfg)
            frame = await _event_frame(server, cfg, key, tenant, account, embedding,
                                       datanode=datanode)
            signature = _frame_signature(frame)
            if signature == last_signature:
                # Nothing has changed since the last frame, so there is nothing to say. The comment
                # keeps the connection alive through an idle proxy; the browser's parser drops
                # every line that is not `data:`, so it costs the page nothing to receive.
                await emit(b": keepalive\n\n")
            else:
                body = json.dumps(frame, default=str).encode("utf-8")
                await emit(b"event: status\ndata: " + body + b"\n\n")
                last_signature = signature

            if (time.time() - started) >= max_age:
                # Say why before going, so a reconnect is not mistaken for a fault.
                await emit(b"event: bye\ndata: {\"reason\": \"stream_max_age\"}\n\n")
                break
            try:
                await asyncio.wait_for(disconnected.wait(), timeout=EVENT_TICK_S)
            except asyncio.TimeoutError:
                pass
    except (asyncio.CancelledError, ConnectionResetError, BrokenPipeError, OSError):
        # The client went away mid-write. Nothing to report: this is how a stream normally ends.
        pass
    finally:
        watcher.cancel()
        try:
            await send({"type": "http.response.body", "body": b"", "more_body": False})
        except Exception:
            pass


# The monitoring assets, by the name a caller asks for. Read from the repo layout relative to this
# module, cached per process like the portal pages.
_GRAFANA_ASSETS = {
    "gateway": ("../docs/ops/matrixark-gateway-dashboard.json", "application/json"),
    "ingestion": ("../docs/ops/matrixark-ingestion-dashboard.json", "application/json"),
    "alerts": ("temporalstore-prometheus/matrixark-gateway-alerts.yml", "text/yaml; charset=utf-8"),
    # Everything that is not the edge. Both files have been in docs/ops all along and were served
    # by nothing, so a customer monitoring from the portal watched the gateway and the importer and
    # had no way to find out the engine was monitorable at all.
    "engine": ("../docs/ops/temporalstore-dashboard.json", "application/json"),
    "engine-alerts": ("../docs/ops/temporalstore-alerts.yml", "text/yaml; charset=utf-8"),
    # The metaserver is the control plane, and 37 of its 41 declared families were on no
    # dashboard at all: convictions, placement shortfalls, shard divergence, safe mode, and
    # the topology version each server last applied. A customer running raft or shared
    # storage could see traffic and storage, and nothing about whether the cluster agreed
    # with itself.
    "cluster": ("../docs/ops/temporalstore-cluster-dashboard.json", "application/json"),
}
_GRAFANA_CACHE: dict[str, Optional[bytes]] = {}

# What each asset covers and -- the part that decides whether it works at all -- which process
# exports the series it queries. The gateway publishes at /v1/metrics; the data node, the metaserver
# and each raft node publish at /metrics on their own listeners. Import the engine dashboard against
# the gateway and all twelve panels come up empty, which reads as an idle deployment rather than as
# a query aimed at the wrong target -- the exact failure these dashboards were shipped to prevent.
_MONITORING_ASSETS: tuple = (
    {"asset": "gateway", "kind": "dashboard", "label": "Gateway",
     "filename": "matrixark-gateway-dashboard.json", "scrape": "gateway",
     "covers": "Edge traffic by route, latency and errors, and whether extraction and retrieval "
               "are really using the models you configured."},
    {"asset": "ingestion", "kind": "dashboard", "label": "Ingestion",
     "filename": "matrixark-ingestion-dashboard.json", "scrape": "gateway",
     "covers": "Import jobs, documents finished and failed, and the failures worth retrying."},
    {"asset": "engine", "kind": "dashboard", "label": "Engine and storage",
     "filename": "temporalstore-dashboard.json", "scrape": "engine",
     "covers": "Raft commit, apply and lease; metaserver scheduler and topology; proxy routing, "
               "admission and quarantine; object and page store lifecycle; cache pressure; data "
               "node lifecycle; and ingestion lag and dead letters. Replica replay and the scale "
               "SLO are no longer listed here: nothing emits their series, so those panels were "
               "blank on every deployment and have been removed."},
    {"asset": "cluster", "kind": "dashboard", "label": "Cluster control plane",
     "filename": "temporalstore-cluster-dashboard.json", "scrape": "engine",
     "covers": "Whether the cluster agrees with itself: the topology version each server last "
               "applied against the current one, safe mode and change-muted switches, "
               "convictions and damage severity, shard divergence, placement and calibration "
               "shortfalls, retention blocked or capped, and per-server records and bytes."},
    {"asset": "alerts", "kind": "rules", "label": "Gateway alert rules",
     "filename": "matrixark-gateway-alerts.yml", "scrape": "gateway",
     "covers": "Retrieval running on hash vectors, extraction with no model call, live "
               "configuration warnings, and imports left sitting for a retry."},
    {"asset": "engine-alerts", "kind": "rules", "label": "Engine alert rules",
     "filename": "temporalstore-alerts.yml", "scrape": "engine",
     "covers": "Raft majority loss and stalled applies, scheduler backlog, proxy quarantine, "
               "cache miss pressure, replay failures and dead letters."},
)


def monitoring_catalogue(datanode_url: str = "") -> Json:
    """The downloadable assets, and the scrape target each one's series actually come from.

    The engine target is taken from the configured datanode URL rather than left as a placeholder:
    the data node serves `/metrics` on the same listener as `/blob`, so this deployment already
    knows the host, and a customer copying the scrape config gets one that resolves.
    """
    engine_host = ""
    try:
        parsed = urlparse(str(datanode_url or ""))
        engine_host = parsed.netloc
    except Exception:  # pragma: no cover - a malformed override falls back to the placeholder
        engine_host = ""
    return {
        "assets": [dict(asset) for asset in _MONITORING_ASSETS
                   if _grafana_asset(asset["asset"])[0] is not None],
        "targets": {
            "gateway": {
                "label": "this gateway",
                "job": "matrixark_gateway",
                "metrics_path": "/v1/metrics",
                "host": "",
                "note": "Aggregate counters only -- no keys and no tenant identifiers -- so it is "
                        "safe to scrape without credentials.",
            },
            "engine": {
                "label": "the engine processes",
                "job": "matrixark_engine",
                "metrics_path": "/metrics",
                "host": engine_host,
                "note": "The data node, the metaserver and every raft node each publish their own "
                        "/metrics. Add each one to this job. Pointing it at the gateway instead "
                        "leaves every engine panel blank, which looks like a quiet cluster rather "
                        "than a query sent to the wrong process.",
            },
        },
    }


def documented_routes() -> list:
    """The route catalogue, each entry saying which edge counter it is counted under.

    Served rather than derived in the page. The label comes from `route_label`, which collapses a
    path to a bounded template; re-implementing that rule in JavaScript would put a second copy of
    it in the tree, and the failure when the two drift is silent -- a number rendered against the
    wrong route reads exactly like a number rendered against the right one.

    `metric_shared_with` names the other documented paths counted under the same label. Eight
    labels cover more than one: a row that showed the shared figure as its own would overstate
    itself by however much the neighbour is used.
    """
    labels: dict = {}
    for entry in ROUTE_DOCS:
        path = str(entry.get("path", ""))
        labels.setdefault(_gwmetrics.route_label(path), set()).add(path)

    out = []
    for entry in ROUTE_DOCS:
        path = str(entry.get("path", ""))
        label = _gwmetrics.route_label(path)
        shared = sorted(labels.get(label, set()) - {path})
        row = dict(entry)
        row["metric"] = label
        if shared:
            row["metric_shared_with"] = shared
        out.append(row)
    return out

def _grafana_asset(name: str) -> Tuple[Optional[bytes], str]:
    """One monitoring asset, or (None, "") when this deployment does not bundle it."""
    entry = _GRAFANA_ASSETS.get(name)
    if entry is None:
        return None, ""
    relative, content_type = entry
    if name in _GRAFANA_CACHE:
        return _GRAFANA_CACHE[name], content_type
    path = os.path.normpath(
        os.path.join(os.path.dirname(os.path.abspath(__file__)), relative))
    try:
        with open(path, "rb") as handle:
            data: Optional[bytes] = handle.read()
    except Exception:  # pragma: no cover - deployments that do not ship the docs tree
        data = None
    _GRAFANA_CACHE[name] = data
    return data, content_type


# Said in full wherever a customer is one click from changing the encoder. The trap is not the
# obvious one: a width change at least has a width to notice. Two encoders of the SAME width mix
# two unrelated vector spaces with nothing anywhere to raise an error.
_EMBEDDING_CHANGE_WARNING = (
    "Changing the embedding model does not re-encode what is already stored. Every existing vector "
    "stays in the old model's space, and vectors from two models cannot be compared -- so those "
    "memories stop matching queries. Note that two encoders are often the SAME WIDTH "
    "(all-MiniLM-L6-v2 and BGE-M3 truncated to 384 are both 384), in which case nothing in the "
    "stack sees a mismatch to complain about. Re-encode the store after changing this, or accept "
    "that everything ingested before the change is no longer semantically searchable."
)


async def _embedding_models_in_store(server: Any, cfg: GatewayConfig, key: Optional[str],
                                     tenant: Optional[str], account: Optional[str]) -> Json:
    """Which encoders the stored vectors were actually made with.

    The question a customer needs answered before switching is not "what can I pick" but "what is
    already in there" -- switching to the model the store was written with is free, and switching
    away from it is not.
    """
    args: Json = {"scope": {}}
    _apply_identity(args, key, tenant, account)
    try:
        result = await asyncio.wait_for(
            asyncio.to_thread(server.call_tool, "matrixark_embedding_status", args),
            cfg.backend_timeout)
    except Exception:
        # Unknown is not the same as none: saying "nothing stored" here would make a destructive
        # change look free.
        return {"known": False,
                "detail": "The backend could not be asked what the stored vectors were made with."}
    if not isinstance(result, dict):
        return {"known": False, "detail": "The backend gave no usable answer."}
    return {
        "known": True,
        "models": result.get("models") or [],
        "dimensions": result.get("dimensions") or [],
        "mixed_dimensions": bool(result.get("mixed_dimensions")),
        "total": result.get("total") or 0,
    }


def embedding_picker_catalogue() -> List[Json]:
    """The measured encoder catalogue, shaped for the picker and annotated with width collisions.

    Built from `encoder_catalog()` rather than from a second list. A hand-written catalogue beside a
    measured one does not stay agreed with it: the one this replaced omitted the whole e5 family --
    the models that actually scored best -- and recommended the encoder the measurement puts fifteen
    points of hit@1 behind e5-small at the same size.

    `same_width_as` is derived here rather than written into any note. Two encoders of the same width
    are the case that raises no error anywhere, and a note only warns about the collisions whoever
    wrote it thought of.
    """
    rows = encoder_catalog()
    by_width: Dict[int, List[str]] = {}
    for row in rows:
        by_width.setdefault(int(row.get("dims") or 0), []).append(str(row.get("id") or ""))
    out: List[Json] = []
    for row in rows:
        width = int(row.get("dims") or 0)
        model = str(row.get("id") or "")
        entry: Json = {
            "model": model,
            "label": row.get("label") or model,
            "dim": width,
            "note": row.get("note") or "",
            "same_width_as": [name for name in by_width.get(width, []) if name != model],
            # The measurement, carried through so the picker can show what a choice costs rather
            # than describing it. A model with no numbers beside it is a name to guess at.
            "hit_at_1": row.get("hit_at_1"),
            "hit_at_5": row.get("hit_at_5"),
            "texts_per_s": row.get("texts_per_s"),
            "vectors_mb_per_doc": row.get("vectors_mb_per_doc"),
            "params_m": row.get("params_m"),
            "window": row.get("window"),
            "recommended": bool(row.get("recommended")),
            "active": bool(row.get("active")),
            # e5 wants "query: " / "passage: " prefixes; the deployment applies them, and a
            # customer comparing it against a model that needs none should know why.
            "needs_prefix": bool(row.get("needs_prefix")),
        }
        out.append(entry)
    return out


def _tenant_policy_module():
    """The policy registry, or None where it is not importable."""
    try:
        import matrixark_tenant_policy as policy_mod  # type: ignore

        return policy_mod
    except Exception:  # pragma: no cover - the registry is optional at import
        try:
            from tools import matrixark_tenant_policy as policy_mod  # type: ignore

            return policy_mod
        except Exception:
            return None


def _policy_view(policy_mod: Any, tenant_id: str, user_id: str) -> Json:
    """Every knob's effective value for this user, where it came from, and whether they may set it.

    The knob's own type, default and description travel with it so the portal renders a control
    rather than a text box, and so the explanation for a setting lives next to the setting rather
    than in a copy that drifts.
    """
    scope: Json = {"tenant_id": tenant_id}
    if user_id:
        scope["user_id"] = user_id
    described = policy_mod.describe_effective_policy(scope)
    knobs: Json = {}
    for name, state in (described.get("knobs") or {}).items():
        knob = policy_mod.KNOBS[name]
        knobs[name] = {
            **state,
            "kind": knob.kind,
            "default": knob.default,
            "choices": sorted(knob.choices) if knob.choices else [],
            "description": knob.description,
        }
    return {
        "status": "ok",
        "tenant": described.get("tenant", tenant_id),
        "user": described.get("user", user_id),
        "knobs": knobs,
        "settable_per_user": sorted(policy_mod.READ_PATH_KNOBS),
        # Every knob can be set for the whole tenant; only the read-path ones for one user.
        "settable_per_tenant": sorted(policy_mod.KNOBS),
        # Where a change would be written, so "saved" can be an honest word.
        "policy_file": policy_mod.policy_file_path(),
        # Said once, here, rather than repeated beside every refused knob.
        "why_some_are_tenant_only": (
            "A store is shared by everyone in a tenant. A setting that changes what gets WRITTEN "
            "into it cannot differ per user without leaving records of two shapes behind — and "
            "unlike a setting, that does not go back when the setting does. Those stay at the "
            "tenant level. The ones offered here decide how your own results are selected and "
            "packed, and touch nobody else's data."),
    }


def _encoder_summary() -> Json:
    """Which encoder the deployment is configured to use, next to the counts.

    A backlog means something different depending on whether an encoder is configured at all: with
    a deterministic provider nothing is waiting because nothing will ever be encoded, and a count of
    zero pending would otherwise read as "all done".
    """
    provider = os.environ.get("MATRIXARK_EMBEDDING_PROVIDER", "deterministic").strip()
    # Asked of the classifier, not decided here. The pair this replaced -- ("", "deterministic") --
    # was the last hand-written copy of that question in this file, and it was the incomplete kind:
    # "local" is a synonym for the hash fallback and a misspelt provider name falls through to it,
    # and both were reported as a working semantic encoder with nothing waiting to be said.
    effect = _gwconfig.embedding_provider_effect(provider)
    if effect == "hash" and _gwconfig.provider_is_unrecognised("embedding", provider):
        note = ("The embedding provider is set to " + repr(provider) + ", which nothing "
                "recognises, so every vector is a hash fallback and nothing is waiting to be "
                "encoded.")
    elif effect == "hash":
        note = ("No encoder is configured, so nothing is waiting to be encoded and nothing ever "
                "will be: every vector is a hash fallback.")
    else:
        note = ""
    return {
        "provider": provider,
        "model": os.environ.get("MATRIXARK_EMBEDDING_MODEL", "").strip(),
        "semantic": effect != "hash",
        "drainer_enabled": os.environ.get("MATRIXARK_EMBED_DRAINER", "").strip().lower()
        in ("1", "true", "yes", "on"),
        "note": note,
    }


def _import_progress() -> Json:
    """What the bulk importer is doing, summarised.

    A running import and a pile of failures waiting for a retry are both states a customer only
    finds by opening the Ingestion page, which is not where anyone starts. Summarised here so the
    landing page can say so, and so a check can be made of it without walking the job list.
    """
    empty: Json = {"running": 0, "documents_total": 0, "documents_done": 0,
                   "documents_failed": 0, "documents_remaining": 0, "retryable": 0,
                   "eta_s": 0, "active": []}
    try:
        import matrixark_ingestion_jobs as _jobs
        snapshots = _jobs.REGISTRY.list()
    except Exception:  # pragma: no cover - the registry is optional at import
        return empty
    active = [s for s in snapshots if s.get("state") in ("running", "queued")]
    result: Json = {
        "running": len(active),
        "documents_total": sum(int(s.get("total") or 0) for s in active),
        "documents_done": sum(int(s.get("done") or 0) for s in active),
        "documents_failed": sum(int(s.get("failed") or 0) for s in snapshots),
        "documents_remaining": sum(int(s.get("remaining") or 0) for s in active),
        # Across concurrent imports the honest figure is the one that finishes LAST, not the sum.
        "eta_s": max([float(s.get("eta_s") or 0) for s in active] or [0]),
        "retryable": sum(int(s.get("retryable_failures") or 0) for s in snapshots),
        "active": [{"job_id": s.get("job_id"), "state": s.get("state"),
                    "total": s.get("total"), "done": s.get("done"),
                    "failed": s.get("failed"), "current": s.get("current"),
                    "eta_s": s.get("eta_s")} for s in active[:5]],
    }
    return result


# What each readiness answer is derived from. The labels are what the page prints, because "ok"
# means something different in each case: a configured encoder can still be serving hash vectors,
# while a counted record is a record.
_CHECK_SOURCE_LABELS: Json = {
    "configuration": "what you configured",
    "measured": "measured in this deployment",
    "engine": "reported by the engine",
}

# Declared per check rather than defaulted, so a check added later has to say which kind of claim it
# is making instead of inheriting the one that looks authoritative.
_CHECK_SOURCES: Json = {
    "extraction": "configuration",
    "extraction_key": "configuration",
    "embedding": "configuration",
    "fail_closed": "configuration",
    "config_warnings": "configuration",
    "ingestion_root": "configuration",
    # Counted a real connection to the backend, not a setting that says where it should be.
    "datanode": "measured",
    "content": "measured",
    # Counts of what is actually there, and traffic actually observed -- these survive a
    # deployment being misconfigured, which is the whole reason the distinction is printed.
    "memory": "measured",
    "metrics": "measured",
    "import_retries": "measured",
    # Settings. "ok" here means the setting is set, not that the thing it configures works.
    "auth": "configuration",
    "single_writer": "configuration",
    "storage_backend": "engine",
}


def _readiness_checks(config_snapshot: Json, counts: Json, cfg: Any,
                      request_total: float = 0.0,
                      imports: Optional[Json] = None,
                      datanode: Optional[str] = None) -> List[Json]:
    """The deployment's setup state as an ordered checklist.

    A customer standing up MatrixArk has to get several independent things right, and every one of
    them fails QUIETLY: an unconfigured extraction provider still answers 200, an unset ingestion
    root only shows up when a bulk import is refused, and an empty store looks exactly like a store
    whose retrieval is broken. Each check below is one of those, phrased as the thing to do next
    rather than as the flag that is off.

    Computed here rather than in the page so the portal makes ONE request for it, and so the same
    answer is available to anyone scripting a deployment check.
    """
    extraction = config_snapshot.get("extraction") or {}
    embedding = config_snapshot.get("embedding") or {}
    checks: List[Json] = []

    def plural(count: int, one: str, many: str = "") -> str:
        return "%d %s" % (count, one if count == 1 else (many or one + "s"))

    def add(check_id: str, title: str, status: str, detail: str,
            href: str = "", action: str = "", how: Optional[List[str]] = None) -> None:
        # `how` is the steps, in order, for the state the check is IN -- an item that is already ok
        # carries none, because the useful thing there is that there is nothing to do.
        source = _CHECK_SOURCES.get(check_id)
        if source is None:
            raise ValueError(
                "readiness check %r has no entry in _CHECK_SOURCES. Every check has to say whether "
                "it measured something or read a setting: a row that read the configuration and a "
                "row that counted records both print as 'ok', and only one of them survives a "
                "deployment that is misconfigured in a way that still answers 200." % check_id)
        checks.append({"id": check_id, "title": title, "status": status, "detail": detail,
                       "href": href, "action": action,
                       "source": source, "source_label": _CHECK_SOURCE_LABELS[source],
                       "how": list(how or []) if status != "ok" else []})

    # First, because if the backend is unreachable the rest of the list is moot -- a reader who
    # sees this at the top stops working through checks about models and keys.
    #
    # Absent means nothing probed, which is not the same as unreachable, so the row says so rather
    # than guessing either way.
    if datanode == "ok":
        add("datanode", "Datanode", "ok",
            "The gateway reached the datanode on its last probe.",
            "/v1/admin/setup", "Deployment")
    elif datanode in ("erroring", "unreachable"):
        add("datanode", "Datanode", "warn",
            ("The datanode answered with an error." if datanode == "erroring"
             else "The gateway could not connect to the datanode.")
            + " Reads and writes cannot be served, and /v1/readyz is answering 503, so this worker "
              "should already be out of rotation.",
            "/v1/admin/setup", "Deployment",
            ["Check the datanode process is running and listening.",
             "Check MATRIXARK_DATANODE_BLOB_URL points at it.",
             "Nothing below this line can work until it does."])
    elif datanode is not None:
        add("datanode", "Datanode", "warn",
            "The datanode reported a state this gateway does not recognise: %r." % datanode,
            "/v1/admin/setup", "Deployment")

    # A name nothing recognises is not "on". It used to pass this test, so the checklist showed
    # a tick and the provider's own name beside it on a deployment running the local rules.
    model_on = _gwconfig.extraction_provider_effect(
        str(extraction.get("provider", ""))) != "rules"
    add("extraction", "Extraction model", "ok" if model_on else "todo",
        ("Ingest calls " + str(extraction.get("model") or extraction.get("provider")) + ".")
        if model_on else
        "No model is configured, so ingest stores only what the local rules extract. This is the "
        "dev default and is rarely what a production deployment wants.",
        "/v1/admin/setup", "Configure",
        how=[
            "Open Setup and pick a provider preset — DeepSeek, OpenAI or Ollama — or fill the "
            "extraction fields by hand.",
            "The base URL must end in /v1: the request is built as <base>/chat/completions, so a "
            "URL without it never reaches the endpoint.",
            "Name the variable that holds the key (DEEPSEEK_API_KEY for DeepSeek), then paste the "
            "key into the field below it.",
            "Save, then restart the gateway: the endpoint and model are read once at startup.",
            "Run Test endpoints. A stored key and an accepted key look identical until you probe.",
        ])

    if model_on:
        add("extraction_key", "Extraction key",
            "ok" if extraction.get("api_key_configured") else "warn",
            (str(extraction.get("api_key_env")) + " is set. Use Test endpoints to confirm the "
             "provider accepts it — a stored key and an accepted key look identical here.")
            if extraction.get("api_key_configured") else
            str(extraction.get("api_key_env")) + " is empty, so every extraction call falls back "
            "to the local rules.", "/v1/admin/setup", "Set the key",
            how=[
                "Setup → Extraction model → paste the key into “Extraction API key”.",
                "It lands in the variable named just above it, which is the one the provider code "
                "reads — so it is live on the next extraction, with no restart.",
                "The key is stored owner-only and is never returned by any read.",
            ])

    semantic = _gwconfig.embedding_provider_effect(
        str(embedding.get("provider", ""))) != "hash"
    add("embedding", "Embedding model", "ok" if semantic else "todo",
        ("Retrieval encodes with " + str(embedding.get("model") or embedding.get("provider")) + ".")
        if semantic else
        "Retrieval is running on hash vectors. It answers, and the answers are not semantic.",
        "/v1/admin/setup", "Configure",
        how=[
            "DeepSeek has no embeddings API, so pair it with an encoder: the Local MiniLM preset "
            "points at the co-located server in tools/context_minilm_embed_server.py.",
            "The base URL must end in /v1 — the request is <base>/embeddings.",
            "A LOCAL encoder still needs a non-empty key value: the call is skipped before it is "
            "attempted when the variable is empty.",
            "Turn on \u201cFail instead of falling back\u201d so an encoder outage errors rather "
            "than quietly returning hash vectors.",
            "Run Test endpoints — it reports the vector dimensions the encoder actually returned.",
        ])

    if semantic:
        add("fail_closed", "Fail closed on the encoder",
            "ok" if embedding.get("require_model_embeddings") else "warn",
            "An unreachable encoder fails the request instead of degrading."
            if embedding.get("require_model_embeddings") else
            "If the encoder becomes unreachable the gateway silently falls back to hash vectors. "
            "Turn on \u201cFail instead of falling back\u201d so an outage is visible.",
            "/v1/admin/setup", "Turn on",
            how=[
                "Setup → Embedding model → “Fail instead of falling back” → on.",
                "It is live immediately; no restart.",
                "The trade is real: an encoder outage becomes failed requests instead of silently "
                "worse retrieval. Failed requests are the ones you find out about.",
            ])

    # The one thing here that is neither a setting nor a count: what the datanode says it actually
    # resolved. Worth its own row because the configured backend and the running one differ silently
    # -- a shared backend with no directory, or MatrixObject on a build without it, both fall
    # through to auto-detection without erroring.
    live_backend = config_snapshot.get("live_storage_backend")
    if live_backend:
        why = str(config_snapshot.get("live_storage_reason") or "").strip()
        add("storage_backend", "Storage backend", "ok",
            "The datanode resolved the " + str(live_backend) + " backend. This is what it is "
            "running, not what was requested"
            + ((" — " + why) if why else
               ". This engine does not publish why it chose it; that is only in its startup log."))
    else:
        add("storage_backend", "Storage backend", "warn",
            "Could not read the datanode's backend, so what this deployment is storing to is "
            "unknown from here. An unreachable datanode and a gateway pointed at the wrong "
            "address look identical at this distance.",
            "/v1/admin/setup", "Check the datanode URL",
            how=["Confirm MATRIXARK_DATANODE_URL points at a datanode that is up.",
                 "That process publishes temporalstore_storage_backend on its own /metrics."])

    warnings = config_snapshot.get("warnings") or []
    add("config_warnings", "Configuration warnings", "ok" if not warnings else "warn",
        "None." if not warnings else
        plural(len(warnings), "warning") + " the gateway can see in its own configuration.",
        "/v1/admin/setup", "Review",
        how=list(warnings))

    root_set = bool(os.environ.get("MATRIXARK_INGESTION_ROOT", "").strip())
    add("ingestion_root", "Ingestion root", "ok" if root_set else "todo",
        "Bulk import can resolve server-side paths."
        if root_set else
        "Unset, so submitting server-side paths is refused outright — bulk import will not run "
        "until this names a directory.", "/v1/admin/setup", "Set it",
        how=[
            "Put the documents somewhere the gateway process can read, then set the ingestion "
            "root to that directory on Setup (group: Ingestion pipeline).",
            "Every path the import accepts is resolved inside it, which is what stops the "
            "endpoint becoming a way to read the filesystem.",
            "Single documents can be uploaded from Explore without this — the bytes come from "
            "your browser, not a server path.",
        ])

    skills, resources = counts.get("skills"), counts.get("resources")
    if skills is None and resources is None:
        add("content", "Skills and resources", "warn",
            "Could not read the catalog with this key.", "/v1/admin/catalog", "Open")
    else:
        total = (skills or 0) + (resources or 0)
        add("content", "Skills and resources", "ok" if total else "todo",
            (plural(skills or 0, "skill") + " and " + plural(resources or 0, "resource")
             + " stored.")
            if total else
            "Nothing has been ingested yet. Import a directory of documents, or POST one to "
            "/v1/ingest.", "/v1/admin/ingestion", "Import",
            how=[
                "Fastest check: Explore → Add → upload one document, and watch it appear in the "
                "catalogue.",
                "For a corpus: set the ingestion root, then Ingestion → preview the selection "
                "before committing to a long run.",
                "Re-importing a document replaces it rather than duplicating it, so a stopped "
                "import can simply be run again.",
            ])

    users = counts.get("users")
    add("memory", "Memory", "ok" if users else ("todo" if users == 0 else "warn"),
        (plural(users, "subject") + (" holds" if users == 1 else " hold") + " memories.")
        if users else
        ("No memories yet. Run a retrieve from Explore once something is ingested — that is the "
         "end-to-end check." if users == 0 else "Could not read the memory list with this key."),
        "/v1/admin/explore", "Explore",
        how=[
            "Explore → Add a memory, with “extract immediately” on so you do not have to wait "
            "for the background pass.",
            "Then Explore → Ask, with a question about what you just wrote.",
            "A retrieve that returns the right thing is the only check that covers ingestion, "
            "extraction, encoding and ranking at once.",
            "An empty result on a non-empty store almost always means the embedding provider is "
            "still deterministic: hash vectors do not match on meaning.",
        ])

    retryable = int((imports or {}).get("retryable") or 0)
    if retryable:
        add("import_retries", "Import failures waiting", "warn",
            plural(retryable, "document") + " failed an import for a reason worth retrying — a "
            "timeout or an exhausted 5xx rather than a rejected request.",
            "/v1/admin/ingestion", "Retry them",
            how=["Open Ingestion and find the job.",
                 "Retry resubmits only those documents, not the whole directory.",
                 "A 4xx failure is listed separately: the request itself is wrong and the same "
                 "send will be rejected again."])

    add("metrics", "Metrics", "ok" if request_total else "warn",
        (str(int(request_total)) + " requests recorded on this worker; /v1/metrics is being "
         "served.") if request_total else
        "No requests recorded yet on this worker. The scrape endpoint is live either way.",
        "/v1/admin/setup", "Scrape config")

    enforced = bool(getattr(cfg, "require_auth", False))
    add("auth", "Authentication", "ok" if enforced else "warn",
        "Requests without a valid key are refused."
        if enforced else
        "Auth is OFF: this deployment answers anonymous requests. That is the developer default; "
        "production should set MATRIXARK_REQUIRE_AUTH=1.", "", "",
        how=[
            "Set MATRIXARK_REQUIRE_AUTH=1 and MATRIXARK_ACCESS_MODE=enforced where the gateway "
            "process is started — this is a launcher decision, so the portal shows it and does "
            "not change it.",
            "Issue keys first, from the API keys page, or the restart locks you out of the "
            "portal's own actions along with everyone else.",
            "Give each workload the narrowest preset that fits: an agent key writes and reads and "
            "cannot delete.",
        ])

    split = _single_writer_warning()
    if split is not None:
        add("single_writer", "Worker processes", "warn", split, "", "")

    return checks


def _worker_count(argv: Optional[list] = None, env: Optional[dict] = None) -> int:
    """How many workers this deployment was started with; 1 when nothing says otherwise.

    Read from the command line, then WEB_CONCURRENCY. Lifted out of the split-store warning so
    there is one answer to the question: that warning is about workers not sharing a STORE, and a
    configuration write has its own reason to care -- a live setting is applied to the environment
    of whichever worker served the request, and no other.
    """
    argv = list(sys.argv if argv is None else argv)
    env = dict(os.environ if env is None else env)
    workers = 0
    for index, token in enumerate(argv):
        if token == "--workers" and index + 1 < len(argv):
            try:
                workers = int(argv[index + 1])
            except ValueError:
                workers = 0
        elif token.startswith("--workers="):
            try:
                workers = int(token.split("=", 1)[1])
            except ValueError:
                workers = 0
    if workers <= 0:
        try:
            workers = int(str(env.get("WEB_CONCURRENCY", "")).strip() or "0")
        except ValueError:
            workers = 0
    return workers if workers > 0 else 1


def _single_writer_warning(
    argv: Optional[list] = None, env: Optional[dict] = None
) -> Optional[str]:
    """Warn when the worker count would silently split a tenant's memory across separate stores.

    With the spawning MCP backend each uvicorn worker starts its OWN `--serve` proxy child over a
    pipe (see matrixark_mcp_rust_proxy_process.ensure_lane_process -- there is no code path that
    dials an existing proxy), and that child owns an embedded store. So `--workers 4` is four
    independent stores: a memory written through one worker is invisible to the other three, with no
    error at any layer.

    A worker count above one is only safe when the workers share a store: a real TS_META_ADDR
    (distributed routing) or an explicit TS_STORAGE_BACKEND. Returns the warning text, or None when
    the configuration is safe -- returning it rather than printing keeps this unit-testable.
    """
    env = dict(os.environ if env is None else env)
    workers = _worker_count(argv, env)
    if workers <= 1:
        return None

    # A shared store makes multiple workers safe.
    if str(env.get("TS_META_ADDR", "")).strip().lower() not in _STORE_SENTINELS:
        return None
    if str(env.get("TS_STORAGE_BACKEND", "")).strip():
        return None
    if str(env.get("TS_SHARED_STORE_DIR", "")).strip():
        return None

    return (
        "MATRIXARK GATEWAY: --workers " + str(workers) + " with the spawning backend gives each "
        "worker its own embedded store, so memories written through one worker are INVISIBLE to the "
        "others. Run --workers 1, or point the workers at one shared store (TS_META_ADDR, or "
        "TS_STORAGE_BACKEND with TS_SHARED_STORE_DIR). Set MATRIXARK_STRICT_SINGLE_WRITER=1 to make "
        "this fatal."
    )


def _enforce_single_writer(argv: Optional[list] = None, env: Optional[dict] = None) -> None:
    """Emit the split-store warning; raise instead when MATRIXARK_STRICT_SINGLE_WRITER is set."""
    warning = _single_writer_warning(argv, env)
    if warning is None:
        return
    env = dict(os.environ if env is None else env)
    if str(env.get("MATRIXARK_STRICT_SINGLE_WRITER", "")).strip().lower() in {"1", "true", "yes", "on"}:
        raise RuntimeError(warning)
    print(warning, file=sys.stderr, flush=True)


async def _read_body_capped(receive: Callable, cap: int) -> Tuple[Optional[bytes], bool]:
    """Read the full request body but bail (None, True) as soon as it exceeds `cap` bytes."""
    chunks: list[bytes] = []
    total = 0
    while True:
        msg = await receive()
        if msg.get("type") != "http.request":
            break
        body = msg.get("body", b"") or b""
        total += len(body)
        if total > cap:
            return None, True
        chunks.append(body)
        if not msg.get("more_body"):
            break
    return b"".join(chunks), False


def _headers_map(scope: Json) -> dict[str, str]:
    return {k.decode("latin-1").lower(): v.decode("latin-1") for k, v in scope.get("headers", [])}


def _ok_body(result: Any) -> Json:
    return result if isinstance(result, dict) else {"result": result}


def _classify_backend_error(exc: Exception) -> int:
    name = exc.__class__.__name__
    if name in _STORAGE_QUOTA_ERRORS:
        return 507
    lowered = str(exc).lower()
    if "storage" in lowered and "quota" in lowered:
        return 507
    if name in _BACKPRESSURE_ERRORS:
        return 429
    if name in _NOT_FOUND_ERRORS:
        return 404
    if name in _INVALID_REQUEST_ERRORS:
        return 400
    return 500


def _finalize_requested(parsed: Json, args: Json) -> bool:
    """True when the caller marks this ingest as a COMPLETE conversation to extract now.

    A single streaming message buffers and defers extraction; a complete conversation is a full unit,
    so `finalize`/`commit` true (or `kind == "conversation"`) triggers the batched extraction
    immediately -- equivalent to ingest + session/commit in one call. Extraction is still ONE batched
    pass over the whole conversation (not per message), and the caller declares granularity; the
    gateway never guesses.
    """
    def truthy(v: Any) -> bool:
        return v is True or str(v).strip().lower() in {"1", "true", "yes", "on"}

    for src in (parsed, args):
        if not isinstance(src, dict):
            continue
        if truthy(src.get("finalize")) or truthy(src.get("commit")):
            return True
        if str(src.get("kind", "")).strip().lower() == "conversation":
            return True
    return False


# ================================================================================================
# Blob proxy (streamed, bounded memory)
# ================================================================================================
async def _blob_put(scope: Json, receive: Callable, send: Callable, method: str,
                    cfg: GatewayConfig, key_str: str, tenant: Optional[str],
                    account: Optional[str] = None) -> None:
    hmap = _headers_map(scope)
    cl_raw = hmap.get("content-length")
    declared = int(cl_raw) if cl_raw and cl_raw.isdigit() else None
    if declared is not None and declared > cfg.max_blob_bytes:
        return await _json(send, 413, {"error": "payload_too_large"})

    dkey = _isolate_key(key_str, tenant, account)
    conn = cfg.blob_connection_factory(cfg)
    chunked = declared is None

    def _start() -> None:
        conn.putrequest(method, f"/blob/{dkey}")
        if declared is not None:
            conn.putheader("Content-Length", str(declared))
        else:
            conn.putheader("Transfer-Encoding", "chunked")
        conn.putheader("Content-Type", hmap.get("content-type", "application/octet-stream"))
        conn.endheaders()

    await asyncio.to_thread(_start)

    total = 0
    while True:
        msg = await receive()
        if msg.get("type") != "http.request":
            break
        body = msg.get("body", b"") or b""
        if body:
            total += len(body)
            if total > cfg.max_blob_bytes:
                await asyncio.to_thread(_safe_close, conn)
                return await _json(send, 413, {"error": "payload_too_large"})
            if chunked:
                frame = f"{len(body):x}\r\n".encode() + body + b"\r\n"
                await asyncio.to_thread(conn.send, frame)
            else:
                await asyncio.to_thread(conn.send, body)
        if not msg.get("more_body"):
            break
    if chunked:
        await asyncio.to_thread(conn.send, b"0\r\n\r\n")

    resp = await asyncio.to_thread(conn.getresponse)
    raw = await asyncio.to_thread(resp.read)
    status = int(getattr(resp, "status", 200) or 200)
    await asyncio.to_thread(_safe_close, conn)

    receipt: Any = None
    if raw:
        try:
            receipt = json.loads(raw)
        except Exception:
            receipt = None
    if not isinstance(receipt, dict):
        receipt = {}
    receipt.setdefault("key", key_str)
    receipt.setdefault("bytes", total)
    out_status = 200 if status < 400 else status
    return await _json(send, out_status, receipt)


async def _blob_get(send: Callable, cfg: GatewayConfig, key_str: str, tenant: Optional[str],
                    account: Optional[str] = None) -> None:
    dkey = _isolate_key(key_str, tenant, account)
    conn = cfg.blob_connection_factory(cfg)

    def _start() -> Any:
        conn.putrequest("GET", f"/blob/{dkey}")
        conn.endheaders()
        return conn.getresponse()

    resp = await asyncio.to_thread(_start)
    status = int(getattr(resp, "status", 200) or 200)
    content_length = _resp_header(resp, "Content-Length")
    content_type = _resp_header(resp, "Content-Type") or "application/octet-stream"

    headers: list[Tuple[bytes, bytes]] = [(b"content-type", str(content_type).encode())]
    if content_length is not None:
        headers.append((b"content-length", str(content_length).encode()))
    await send({"type": "http.response.start", "status": status, "headers": headers})

    while True:
        chunk = await asyncio.to_thread(resp.read, 65536)
        if not chunk:
            await send({"type": "http.response.body", "body": b"", "more_body": False})
            break
        await send({"type": "http.response.body", "body": chunk, "more_body": True})
    await asyncio.to_thread(_safe_close, conn)


def _resp_header(resp: Any, name: str) -> Optional[str]:
    try:
        return resp.getheader(name)
    except Exception:
        return None


def _safe_close(conn: Any) -> None:
    try:
        conn.close()
    except Exception:
        pass


# ================================================================================================
# Combined upload-and-ingest (POST /v1/ingest_file): stream file body -> blob tier -> ingest.
# ================================================================================================
_INGEST_CONTENT_TYPES = {
    "md": "text/markdown", "markdown": "text/markdown", "txt": "text/plain",
    "text": "text/plain", "pdf": "application/pdf", "json": "application/json",
    "html": "text/html", "csv": "text/csv",
}

# Accepted values for the sharing/visibility knob on POST /v1/ingest_file. Surfaced as
# the top-level ``sharing_scope`` key on the synthesized /v1/ingest body (same knob the
# SDK sends). Mirrors matrixark_ingest_client.VALID_SHARING_SCOPES.
_VALID_SHARING_SCOPES = ("private_user", "tenant_shared", "global_shared")


def _ingest_content_type(resource_type: str) -> str:
    return _INGEST_CONTENT_TYPES.get((resource_type or "").lower(), "application/octet-stream")


def _infer_ingest_resource_type(rtype_header: Optional[str], filename: Optional[str]) -> str:
    """X-Resource-Type wins; else the suffix of X-Filename; else ``md`` (skill default)."""
    if rtype_header:
        return rtype_header.strip().lstrip(".").lower()
    if filename:
        suffix = os.path.splitext(filename)[1].lstrip(".").lower()
        if suffix:
            return suffix
    return "md"


async def _spool_and_hash(receive: Callable, cfg: GatewayConfig) -> Tuple[Optional[str], int, str, bool]:
    """Stream the request body to a temp file while hashing, bounded memory.

    Returns ``(spool_path, size, sha256_hex, too_big)``. When ``too_big`` is True the
    partial spool is already removed and ``spool_path`` is None. Reads in native ASGI
    chunks and writes straight through to disk -- nothing but one chunk is ever held
    in memory, so a multi-GB file costs flat memory.
    """
    spool_dir = cfg.ingest_spool_dir or None
    fd, path = tempfile.mkstemp(prefix="matrixark-ingest-", suffix=".spool", dir=spool_dir)
    hasher = hashlib.sha256()
    total = 0
    try:
        with os.fdopen(fd, "wb") as fh:
            while True:
                msg = await receive()
                if msg.get("type") != "http.request":
                    break
                body = msg.get("body", b"") or b""
                if body:
                    total += len(body)
                    if total > cfg.max_blob_bytes:
                        fh.close()
                        _safe_unlink(path)
                        return None, total, "", True
                    hasher.update(body)
                    await asyncio.to_thread(fh.write, body)
                if not msg.get("more_body"):
                    break
    except Exception:
        _safe_unlink(path)
        raise
    return path, total, hasher.hexdigest(), False


async def _stream_spool_to_datanode(cfg: GatewayConfig, spool_path: str, size: int,
                                    dkey: str, content_type: str) -> Tuple[int, bytes]:
    """PUT the spooled file to the datanode ``/blob/<dkey>`` using the SAME streamed,
    bounded-memory approach as ``_blob_put`` (Content-Length known -> raw stream)."""
    conn = cfg.blob_connection_factory(cfg)

    def _start() -> None:
        conn.putrequest("PUT", f"/blob/{dkey}")
        conn.putheader("Content-Length", str(size))
        conn.putheader("Content-Type", content_type)
        conn.endheaders()

    await asyncio.to_thread(_start)
    with open(spool_path, "rb") as fh:
        while True:
            chunk = await asyncio.to_thread(fh.read, 65536)
            if not chunk:
                break
            await asyncio.to_thread(conn.send, chunk)
    resp = await asyncio.to_thread(conn.getresponse)
    raw = await asyncio.to_thread(resp.read)
    status = int(getattr(resp, "status", 200) or 200)
    await asyncio.to_thread(_safe_close, conn)
    return status, raw


def _safe_unlink(path: Optional[str]) -> None:
    if not path:
        return
    try:
        os.unlink(path)
    except Exception:
        pass


async def _ingest_file(app: Callable, scope: Json, receive: Callable, send: Callable,
                       cfg: GatewayConfig, key_str_unused: Optional[str],
                       tenant: Optional[str], account: Optional[str]) -> None:
    """Serve ``POST /v1/ingest_file``: stream the raw file body to the blob tier
    (content-addressed, tenant-isolated, bounded memory), then invoke the SAME
    ingest handler ``/v1/ingest`` uses by re-dispatching a synthesized JSON request
    through the app -- so there is no duplicated ingest/finalize/extraction logic.
    """
    hmap = _headers_map(scope)
    filename = hmap.get("x-filename")
    kind = (hmap.get("x-resource-kind") or "skill").strip().lower() or "skill"
    resource_type = _infer_ingest_resource_type(hmap.get("x-resource-type"), filename)
    content_type = hmap.get("content-type") or _ingest_content_type(resource_type)

    # Optional sharing/visibility level. X-Sharing-Scope (X-Visibility alias) -> the
    # top-level ``sharing_scope`` ingest key. Validate early (before spooling) so a bad
    # value fails fast with 400; omitted -> absent (server default applies).
    sharing_scope = hmap.get("x-sharing-scope") or hmap.get("x-visibility")
    if sharing_scope:
        sharing_scope = sharing_scope.strip()
        if sharing_scope not in _VALID_SHARING_SCOPES:
            return await _json(send, 400, {
                "error": "invalid_sharing_scope",
                "detail": f"unknown sharing_scope {sharing_scope!r} (use one of "
                          f"{', '.join(_VALID_SHARING_SCOPES)})",
            })
    else:
        sharing_scope = None

    cl_raw = hmap.get("content-length")
    declared = int(cl_raw) if cl_raw and cl_raw.isdigit() else None
    if declared is not None and declared > cfg.max_blob_bytes:
        return await _json(send, 413, {"error": "payload_too_large"})

    spool_path, size, sha, too_big = await _spool_and_hash(receive, cfg)
    if too_big:
        return await _json(send, 413, {"error": "payload_too_large"})
    try:
        # Content-addressed key, server-side: resources/<sha2>/<sha256>. Same bytes ->
        # same key -> dedup. Tenant-isolate exactly like the /v1/blob route.
        logical_key = f"resources/{sha[:2]}/{sha}"
        dkey = _isolate_key(logical_key, tenant, account)
        try:
            status, raw = await _stream_spool_to_datanode(cfg, spool_path, size, dkey, content_type)
        except Exception as exc:
            # The blob tier being unreachable is an ordinary operational state -- a datanode
            # restarting, a wrong MATRIXARK_DATANODE_URL -- and it was the one failure on this
            # route that escaped as an unhandled exception. The caller got a bare 500 with no
            # reason and the server logged a stack trace, which reads like a bug in the upload
            # rather than a backend that is down.
            _LOG.warning("ingest_file: blob store unreachable: %s", exc)
            return await _json(send, 502, {
                "error": "blob_store_unreachable",
                "detail": "Could not reach the blob tier at %s: %s"
                          % (getattr(cfg, "datanode_url", "the configured datanode"),
                             exc.__class__.__name__),
            })
        if status >= 400:
            detail: Any = None
            if raw:
                try:
                    detail = json.loads(raw)
                except Exception:
                    detail = raw[:256].decode("utf-8", "replace")
            return await _json(send, status if status in (413, 429, 507) else 502,
                               {"error": "blob_store_failed", "detail": detail})
    finally:
        _safe_unlink(spool_path)

    # Re-dispatch through the SAME app as a normal /v1/ingest, carrying the auth header
    # so identity/scope resolve identically. The body is a tiny pointer, not the bytes.
    raw_uri = f"temporalstore://{logical_key}"
    ingest_body: Json = {"kind": kind, "raw_uri": raw_uri, "resource_type": resource_type}
    if _truthy_header(hmap.get("x-wait")):
        ingest_body["finalize"] = True
    # X-Scope is documented (docs/enterprise_onboarding.html) as a JSON scope OBJECT --
    # {"user_id":"alice","session_id":"s-42"} -- so parse it. Passing the raw header string
    # through reached _apply_identity's string branch, which reads a bare string as a NAMESPACE
    # LABEL: the upload was filed under namespace `acme/{"user_id":"alice"}` and the user_id was
    # dropped. No error at any layer, and the file lands in the wrong scope. A string that is not
    # a JSON object still means a namespace label, so anyone relying on that keeps it.
    x_scope = hmap.get("x-scope")
    if x_scope:
        text = x_scope.strip()
        if text.startswith("{"):
            try:
                parsed_scope = json.loads(text)
            except ValueError:
                return await _json(send, 400, {
                    "error": "invalid_scope",
                    "detail": "X-Scope looks like JSON but does not parse; send a scope object "
                              "such as {\"user_id\": \"alice\"}",
                })
            if not isinstance(parsed_scope, dict):
                return await _json(send, 400, {
                    "error": "invalid_scope",
                    "detail": "X-Scope must be a JSON object when it starts with '{'",
                })
            ingest_body["scope"] = parsed_scope
        else:
            ingest_body["scope"] = text
    if sharing_scope is not None:
        ingest_body["sharing_scope"] = sharing_scope

    auth_headers = [(k, v) for (k, v) in scope.get("headers", [])
                    if k.decode("latin-1").lower() in ("authorization", "x-api-key")]
    payload = json.dumps(ingest_body).encode("utf-8")
    inner_scope = {
        "type": "http", "method": "POST", "path": "/v1/ingest",
        "headers": auth_headers + [(b"content-type", b"application/json")],
    }
    delivered = {"done": False}

    async def inner_receive() -> Json:
        if not delivered["done"]:
            delivered["done"] = True
            return {"type": "http.request", "body": payload, "more_body": False}
        return {"type": "http.request", "body": b"", "more_body": False}

    return await app(inner_scope, inner_receive, send)


def _truthy_header(value: Optional[str]) -> bool:
    return bool(value) and value.strip().lower() in ("1", "true", "yes", "on")


# ================================================================================================
# Direct network path (B): pooled async JSON client to a target base URL
# ================================================================================================
class DirectBackendClient:
    """Reusable, target-agnostic pooled async JSON client.

    POSTs a JSON body to `{base_url}{path}` and returns `(status, dict)`. A small
    thread-safe pool of persistent `http.client` connections is reused across
    requests so concurrent `/v1` traffic amortizes TCP/connect cost instead of
    spawning a subprocess per call (the serial MCP-adapter failure mode). Sync
    I/O runs in a worker thread (`asyncio.to_thread`) to keep the event loop free.

    `target` is whatever base URL you point it at -- the Rust service proxy
    (default, so the proxy owns tenant_hash + shard routing) or a datanode
    directly ("one hop less"). `connection_factory` is injectable so tests stub
    the transport with no network.
    """

    def __init__(self, base_url: str, *, timeout: float = 30.0, pool_size: int = 64,
                 connection_factory: Optional[Callable[[], Any]] = None) -> None:
        parsed = urlparse(str(base_url))
        self.scheme = parsed.scheme or "http"
        self.host = parsed.hostname or "127.0.0.1"
        self.port = parsed.port or (443 if self.scheme == "https" else 80)
        self.base_path = (parsed.path or "").rstrip("/")
        self.timeout = float(timeout)
        self.pool_size = max(1, int(pool_size))
        self._factory = connection_factory or self._default_factory
        self._pool: list[Any] = []
        self._lock = threading.Lock()

    def _default_factory(self) -> Any:
        import http.client

        if self.scheme == "https":
            return http.client.HTTPSConnection(self.host, self.port, timeout=self.timeout)
        return http.client.HTTPConnection(self.host, self.port, timeout=self.timeout)

    def _acquire(self) -> Any:
        with self._lock:
            if self._pool:
                return self._pool.pop()
        return self._factory()

    def _release(self, conn: Any) -> None:
        with self._lock:
            if len(self._pool) < self.pool_size:
                self._pool.append(conn)
                return
        _safe_close(conn)

    def _post_sync(self, path: str, payload: Json) -> Tuple[int, Json]:
        body = json.dumps(payload).encode("utf-8")
        conn = self._acquire()
        try:
            conn.putrequest("POST", self.base_path + path)
            conn.putheader("Content-Type", "application/json")
            conn.putheader("Content-Length", str(len(body)))
            conn.endheaders()
            conn.send(body)
            resp = conn.getresponse()
            raw = resp.read()
            status = int(getattr(resp, "status", 200) or 200)
        except Exception:
            _safe_close(conn)
            raise
        else:
            self._release(conn)
        parsed: Any = json.loads(raw) if raw else {}
        return status, parsed if isinstance(parsed, dict) else {"result": parsed}

    async def post_json(self, path: str, payload: Json) -> Tuple[int, Json]:
        return await asyncio.to_thread(self._post_sync, path, payload)


def _build_direct_client(cfg: GatewayConfig) -> DirectBackendClient:
    return DirectBackendClient(
        cfg.direct_backend_url,
        timeout=cfg.backend_timeout,
        pool_size=int(cfg.direct_pool_size),
        connection_factory=cfg.direct_connection_factory,
    )


def _parse_prom_labels(line: str) -> Json:
    """Labels of one Prometheus sample, honouring backslash escapes.

    Splitting on "," and stripping quotes tears a value in half at its first escaped quote, and the
    reason label carries paths and endpoint URLs, so that is a matter of when rather than whether.
    """
    start = line.find("{")
    end = line.rfind("}")
    if start < 0 or end < start:
        return {}
    out: Json = {}
    key = ""
    buf: List[str] = []
    in_value = False
    escaped = False
    for ch in line[start + 1:end]:
        if not in_value:
            if ch == "=":
                key = "".join(buf).strip()
                buf = []
            elif ch == '"' and not key:
                continue
            elif ch == '"':
                in_value = True
            elif ch == ",":
                buf = []
            else:
                buf.append(ch)
            continue
        if escaped:
            buf.append({"n": "\n", "\\": "\\", '"': '"'}.get(ch, ch))
            escaped = False
        elif ch == "\\":
            escaped = True
        elif ch == '"':
            out[key] = "".join(buf)
            key, buf, in_value = "", [], False
        else:
            buf.append(ch)
    return out


def _configured_engine_settings() -> list:
    """Engine variables this deployment has explicitly set, for the plan to disclose.

    Only names, never values: the plan and the artifact it produces travel, and a tuning value is
    the least of what could ride along if this returned the settings themselves.
    """
    try:
        import matrixark_gateway_config as gwconfig

        snapshot = gwconfig.snapshot()
    except Exception:  # pragma: no cover - the plan is still worth producing without this
        return []
    names = []
    for items in (snapshot.get("groups") or {}).values():
        for item in items:
            env = item.get("env") or ""
            if env.startswith("TS_") and item.get("source") in ("portal", "environment"):
                names.append(env)
    return sorted(set(names))


def _engine_metrics_text(cfg: GatewayConfig) -> Optional[str]:
    """The datanode's /metrics response, or None if it could not be read.

    Lifted out of the backend probe because the same response answers more than one question and
    the overview should not ask twice for it. Everything that reads it is a pure parser over this
    text, so a series the engine does not publish is absent rather than fatal to the others.
    """
    try:
        conn = cfg.blob_connection_factory(cfg)
        conn.putrequest("GET", "/metrics")
        conn.endheaders()
        resp = conn.getresponse()
        body = b""
        try:
            body = resp.read() or b""
        except Exception:
            pass
        status = int(getattr(resp, "status", 0) or 0)
        _safe_close(conn)
        if status >= 400:
            return None
        return body.decode("utf-8", "replace")
    except Exception:
        return None


def _prom_samples(text: str, name: str):
    """(labels, value) for every sample of one series. A malformed line is skipped, not fatal."""
    prefix = name + "{"
    for line in text.splitlines():
        if not line.startswith(prefix):
            continue
        close = line.rfind("}")
        if close < 0:
            continue
        try:
            value = float(line[close + 1:].strip().split()[0])
        except (IndexError, ValueError):
            continue
        yield _parse_prom_labels(line), value


def _engine_footprint(text: Optional[str]) -> Json:
    """What the engine says it is holding, summed across shards.

    `available` is false when the engine published none of it, which is a fact about the
    deployment rather than an error -- an older engine, or one that has not served a request yet.
    Reporting zero for that would be a number, and a wrong one.

    `cache_memory_bytes` is the tier the two cache-size settings actually trade: their help calls
    it "a direct trade of footprint against lookup latency" and this is the footprint half.
    Logical and physical store bytes are carried together because the ratio between them is the
    compression a deployment is really getting, which no single number shows.
    """
    if not text:
        return {"available": False}
    found = False
    cache: Json = {}
    for labels, value in _prom_samples(text, "temporalstore_cache_bytes"):
        tier = labels.get("tier") or ""
        if not tier:
            continue
        found = True
        cache[tier] = cache.get(tier, 0.0) + value
    logical = physical = 0.0
    for labels, value in _prom_samples(text, "temporalstore_storage_slot_bytes"):
        kind = labels.get("kind") or ""
        if kind == "logical":
            found = True
            logical += value
        elif kind == "physical":
            found = True
            physical += value
    if not found:
        return {"available": False}
    out: Json = {
        "available": True,
        "cache_memory_bytes": int(cache.get("memory", 0.0)),
        "cache_disk_bytes": int(cache.get("disk", 0.0)),
        "cache_compression_saved_bytes": int(cache.get("compression_saved", 0.0)),
        "store_logical_bytes": int(logical),
        "store_physical_bytes": int(physical),
    }
    # Only when both halves are real: a ratio against a zero denominator is not a compression
    # figure, and 0.0 beside "compression" reads as "none", which is a different claim.
    if physical > 0 and logical > 0:
        out["store_compression_ratio"] = round(logical / physical, 2)
    return out


def _worker_resident() -> Json:
    """This worker's resident memory, and where the number came from.

    The source is reported because the two ways of asking do not agree about units:
    `ru_maxrss` is kilobytes on Linux and BYTES on macOS, and a panel that picks wrong is out by a
    factor of 1024 while looking entirely plausible. /proc is unambiguous, so it is preferred and
    named.
    """
    try:
        with open("/proc/self/status", encoding="utf-8") as handle:
            fields: Json = {}
            for line in handle:
                if line.startswith(("VmRSS:", "VmHWM:")):
                    key, value = line.split(":", 1)
                    fields[key] = int(value.strip().split()[0]) * 1024
        if fields.get("VmRSS"):
            return {"resident_bytes": int(fields["VmRSS"]),
                    "peak_bytes": int(fields.get("VmHWM") or fields["VmRSS"]),
                    "source": "/proc/self/status"}
    except (OSError, ValueError, IndexError):
        pass
    try:
        import resource as _resource

        raw = _resource.getrusage(_resource.RUSAGE_SELF).ru_maxrss
        peak = int(raw) * (1 if sys.platform == "darwin" else 1024)
        # Only the peak is available this way. Reporting it as "resident" would overstate a worker
        # that has since given memory back.
        return {"resident_bytes": None, "peak_bytes": peak, "source": "ru_maxrss"}
    except Exception:  # pragma: no cover - a platform with neither
        return {"resident_bytes": None, "peak_bytes": None, "source": "unavailable"}


def _footprint_summary(cfg: GatewayConfig, text: Optional[str] = None) -> Json:
    """What this deployment is holding, on the page that asks an operator to trade it.

    Six settings ask for that trade in their own words -- the page and block index caches call it
    "a direct trade of footprint against lookup latency", `generate_embeddings` prices itself at
    "~13% of resident memory", `share_repeated_values` quotes 761 MB against 1.1 GB -- and the
    portal showed no footprint at all. A control whose stated unit is invisible on the same page
    is a decision the customer is asked to make blind.

    The worker figures are ONE worker's and are never summed. Resident sets share pages, so adding
    four workers together produces a number larger than the machine is using; the worker count is
    reported beside them so the reader can see there are others without being handed a total that
    would be wrong.
    """
    worker = _worker_resident()
    worker["workers"] = _worker_count()
    return {
        "worker": worker,
        "engine": _engine_footprint(_engine_metrics_text(cfg) if text is None else text),
    }


def _probe_storage_backend(cfg: GatewayConfig, text: Optional[str] = None) -> Optional[Json]:
    """Which storage backend the datanode actually resolved. None => could not determine.

    Read from the engine instead of re-derived here, because the two can disagree and the engine is
    the one that is right: `TS_STORAGE_BACKEND=shared` with no directory, or `matrixobject` on a
    build without the feature, both fall through to auto-detection without erroring, so a
    deployment routinely runs a backend nobody selected. The engine publishes what it chose as
    `temporalstore_storage_backend{backend=...}`; the *reason* it chose it exists only in a startup
    log line, so this reports the outcome and the portal says plainly that the reason is not
    available over HTTP.
    """
    try:
        if text is None:
            text = _engine_metrics_text(cfg)
        if text is None:
            return None
        outcome: Json = {}
        reason = ""
        for line in text.splitlines():
            if line.startswith("temporalstore_storage_backend_info{"):
                labels = _parse_prom_labels(line)
                reason = labels.get("reason", "") or reason
            elif line.startswith("temporalstore_storage_backend{"):
                labels = _parse_prom_labels(line)
                if labels.get("backend"):
                    outcome = {"backend": labels["backend"],
                               "replication": labels.get("replication", ""),
                               "source": "datanode /metrics"}
        if not outcome:
            return None
        # Absent on an engine older than the series. That is a fact about the deployment, not a
        # failure, so the caller distinguishes "no reason published" from "could not reach it".
        outcome["reason"] = reason
        return outcome
    except Exception:
        return None


def _probe_datanode(cfg: GatewayConfig) -> str:
    """What the datanode is doing, by name: "ok", "erroring" or "unreachable".

    It used to return a tri-state bool whose two failure states were reported under names that had
    them the wrong way round. None meant the connection failed -- nothing listening -- and was
    reported as "unknown"; False meant the datanode answered with a 5xx and was reported as
    "unreachable". The reassuring word described the worse state.

    There is no fourth state for "no datanode configured": `datanode_url` always resolves, so a
    connection failure is a failure to reach something that should be there.
    """
    try:
        conn = cfg.blob_connection_factory(cfg)
        conn.putrequest("GET", "/health")
        conn.endheaders()
        resp = conn.getresponse()
        try:
            resp.read()
        except Exception:
            pass
        status = int(getattr(resp, "status", 0) or 0)
        _safe_close(conn)
        return "ok" if status < 500 else "erroring"
    except Exception:
        # Could not connect at all. This is the state that was called "unknown", and it is the
        # least ambiguous of the three.
        return "unreachable"


# ================================================================================================
# The app
# ================================================================================================
def _install_sized_executor() -> Optional[int]:
    """Install a sized default `ThreadPoolExecutor` on the running loop (once per worker process).

    The synchronous backend (`server.call_tool`) and blob I/O all run through `asyncio.to_thread`,
    whose default executor caps at ~`min(32, cpu+4)` threads. Under high request concurrency that
    cap serializes backend calls and inflates tail latency well before CPU saturates. Setting
    `MATRIXARK_GATEWAY_THREADS=N` sizes the pool explicitly so the gateway can keep more backend
    calls in flight. No-op (keeps asyncio's default) when the env var is unset/invalid.
    """
    raw = os.environ.get("MATRIXARK_GATEWAY_THREADS")
    if not raw:
        return None
    try:
        n = int(raw)
    except (TypeError, ValueError):
        return None
    if n <= 0:
        return None
    from concurrent.futures import ThreadPoolExecutor

    loop = asyncio.get_running_loop()
    loop.set_default_executor(ThreadPoolExecutor(max_workers=n, thread_name_prefix="mtx-gw"))
    return n


def _direct_context_body(tool: str, args: Json, finalize: bool) -> Tuple[str, Json]:
    """Translate the parsed `/v1` args into a proxy `/context/*` high-level body.

    Returns `(proxy_path, body)`. The gateway performs NO hashing: it forwards the
    raw `scope` identifiers and lets the proxy derive `tenant_hash` + `shard_id`.

    Fast-ack split: a plain streaming ingest goes to `/context/ingest` (raw store,
    no extraction); a finalize/commit goes to `/context/extract` (batched
    extraction) which replays the buffered raw events when no messages are given.
    """
    scope = args.get("scope")
    if not isinstance(scope, dict):
        scope = {"tenant_id": scope} if scope else {}
    if tool == "matrixark_retrieve":
        body: Json = {"scope": scope, "query": args.get("query", "")}
        for key in ("start_time_ms", "end_time_ms", "max_events", "node_hashes"):
            if args.get(key) is not None:
                body[key] = args[key]
        return "/context/retrieve", body

    if tool == "matrixark_session_commit":
        # Commit replays and extracts the buffered raw events for the scope.
        return "/context/extract", {"scope": scope}

    # matrixark_ingest
    body = {"scope": scope}
    messages = args.get("messages")
    records = args.get("records") or args.get("sources")
    if isinstance(messages, list) and messages:
        body["messages"] = messages
    if isinstance(records, list) and records:
        body["records"] = records
    for key in ("query", "start_time_ms", "end_time_ms", "max_events", "provider"):
        if args.get(key) is not None:
            body[key] = args[key]
    # finalize:true / kind=conversation -> extract now; else fast raw store.
    return ("/context/extract" if finalize else "/context/ingest"), body


async def _dispatch_direct(client: "DirectBackendClient", cfg: GatewayConfig, tool: str,
                           parsed: Json, args: Json, send: Callable,
                           rl_headers: list[Tuple[bytes, bytes]], *,
                           n_records: Optional[int], n_messages: Optional[int]) -> None:
    """Serve one `/v1/ingest`, `/v1/retrieve`, or `/v1/session/commit` over the direct path."""
    finalize = tool == "matrixark_ingest" and _finalize_requested(parsed, args)
    path, body = _direct_context_body(tool, args, finalize)
    try:
        status, data = await asyncio.wait_for(client.post_json(path, body), cfg.backend_timeout)
    except asyncio.TimeoutError:
        return await _json(send, 504, {"error": "backend_timeout",
                           "detail": f"backend did not respond within {cfg.backend_timeout}s"}, rl_headers)
    except Exception as exc:
        return await _json(send, _classify_backend_error(exc),
                           _failure(scope, "backend_error", exc), rl_headers)

    if status >= 400:
        return await _json(send, status if status in (429, 507) else 502,
                           {"error": "backend_error", "detail": data}, rl_headers)

    if tool == "matrixark_ingest":
        accepted = n_records if n_records is not None else (n_messages or 0)
        out: Json = {"accepted": accepted, "scope": args.get("scope"), "result": data}
        # A finalize goes through /context/extract, so it is already extracted.
        if finalize:
            out["finalized"] = True
            out["extraction"] = data
        return await _json(send, 202, out, rl_headers)
    # session/commit (200) and retrieve (200) pass the backend body through.
    return await _json(send, 200, _ok_body(data), rl_headers)


def make_v1_app(server: Any, config: Any = None) -> Callable[..., Awaitable[None]]:
    """Return the `/v1/*` gateway ASGI app fronting an already-built MatrixArk `server`.

    Non-`/v1` paths fall through to the legacy `make_asgi_app(server)` so `/api/*`, `/mcp` and the
    bare `/healthz` keep working unchanged.
    """
    cfg = _coerce_config(config)
    _warn_if_auth_disabled(cfg)  # one-time, non-blocking; anonymous access still allowed
    legacy = make_asgi_app(server)
    limiter = _RateLimiter(cfg)
    meter = _UsageMeter(
        getattr(cfg, "usage_file", "") or "",
        flush_every=int(getattr(cfg, "usage_flush_every", 50)),
        flush_interval_s=float(getattr(cfg, "usage_flush_interval_s", 5.0)),
    )
    executor_state = {"installed": False}
    executor_lock = threading.Lock()
    # Direct network path (B): one shared pooled client per worker when enabled.
    direct_client = _build_direct_client(cfg) if cfg.direct_backend else None
    # A multi-worker deployment on the spawning backend silently splits the store; say so
    # loudly here rather than letting writes scatter across per-worker embedded stores.
    _enforce_single_writer()
    # Seed the process environment from what the portal stored, WITHOUT overriding anything the
    # launcher exported: default < stored config < explicit environment, the same precedence
    # matrixark_load_config uses. Best-effort -- a deployment must still start with an unreadable
    # config file.
    try:
        _gwconfig.apply_boot()
    except Exception:  # pragma: no cover - never block startup on stored config
        _LOG.warning("MATRIXARK GATEWAY: stored runtime config could not be applied", exc_info=True)

    async def _serve(scope: Json, receive: Callable, send: Callable) -> None:
        # ASGI lifespan: start the background stream-materializer on startup (so a non-finalized
        # streaming ingest becomes retrievable on its own) and stop it cleanly on shutdown. Started
        # per uvicorn worker process; idempotent with the eager start in create_v1_app().
        if scope.get("type") == "lifespan":
            while True:
                message = await receive()
                mtype = message.get("type")
                if mtype == "lifespan.startup":
                    try:
                        ensure = getattr(server, "ensure_stream_materialize_worker", None)
                        if callable(ensure):
                            ensure()
                    except Exception:  # never block startup on the materializer
                        pass
                    await send({"type": "lifespan.startup.complete"})
                elif mtype == "lifespan.shutdown":
                    try:
                        close = getattr(server, "close", None)
                        if callable(close):
                            close()
                    except Exception:
                        pass
                    await send({"type": "lifespan.shutdown.complete"})
                    return
            return
        if scope.get("type") != "http":
            return

        # Set at the top of every HTTP request, so the response helpers below can negotiate an
        # encoding without being handed the scope. Before the legacy branch as well: those
        # responses go through the same helpers.
        _ACCEPT_ENCODING.set(_headers_map(scope).get("accept-encoding", ""))
        # Cleared per request: a connection can serve several, and a token left behind would
        # attach the previous failure's log entry to this one.
        _INCIDENT.set("")

        # First HTTP request on this loop/worker installs the sized threadpool (if configured).
        if not executor_state["installed"]:
            with executor_lock:
                if not executor_state["installed"]:
                    _install_sized_executor()
                    executor_state["installed"] = True
        path = scope.get("path", "")
        method = scope.get("method", "")

        if not (path == "/v1" or path.startswith("/v1/")):
            return await legacy(scope, receive, send)

        # ---- health (no auth) ---------------------------------------------------------------
        if method == "GET" and path == "/v1/healthz":
            return await _json(send, 200, {"status": "ok"})
        if method == "GET" and path == "/v1/readyz":
            # Answers 503 when the datanode cannot serve. It used to answer 200 with
            # `"ready": true` whatever the probe found, so a gateway with an unreachable backend
            # stayed in rotation and was handed requests it could not fulfil -- the one thing a
            # readiness probe exists to prevent.
            #
            # `/v1/healthz` is unchanged and still answers 200 whenever the process is alive: a
            # liveness probe that fails on a dependency gets the container restarted, which fixes
            # nothing and loses whatever it was doing.
            datanode = await asyncio.to_thread(_probe_datanode, cfg)
            # Recorded so monitoring can see it too. The orchestrator acts on the status code;
            # without a series nobody can alert on a backend that has been down for two minutes.
            _gwmetrics.METRICS.note_datanode(datanode)
            ready = datanode == "ok"
            return await _json(send, 200 if ready else 503,
                               {"ready": ready, "datanode": datanode})

        # ---- key-management portal UI (static HTML, no auth to FETCH) ------------------------
        # Returns the self-contained portal page. Fetching the static page needs no auth; every
        # ACTION on it calls an admin-gated JSON endpoint, so the page is inert without a valid
        # admin key. Kept before the data routes so it never touches auth/metering/quota.
        if method == "GET" and path == "/v1/admin/portal":
            return await _page(send, scope, _portal_html_bytes())

        # ---- setup + catalog pages (static HTML, no auth to FETCH) ---------------------------
        # Same posture as the key portal: the page is inert without an admin key, because every
        # action on it calls an admin-gated JSON endpoint.
        if method == "GET" and path in ("/v1/admin", "/v1/admin/"):
            return await _page(send, scope, _overview_portal_html_bytes())
        if method == "GET" and path == "/v1/admin/explore":
            return await _page(send, scope, _explore_portal_html_bytes())
        if method == "GET" and path == "/v1/admin/api":
            return await _page(send, scope, _api_portal_html_bytes())

        # ---- the API surface, as data (no auth: it is the published contract) -----------------
        # Served rather than written down separately so what a customer reads is what this process
        # serves. No credentials: this is documentation, and every route it names enforces its own.
        if method == "GET" and path == "/v1/admin/routes":
            return await _json(send, 200, {"status": "ok", "routes": documented_routes()})
        if method == "GET" and path == "/v1/admin/setup":
            return await _page(send, scope, _setup_portal_html_bytes())
        if method == "GET" and path == "/v1/admin/catalog":
            return await _page(send, scope, _catalog_portal_html_bytes())

        # ---- per-key usage read (auth + admin scope) ----------------------------------------
        # Returns the in-process edge counters (per-key totals, ingest/retrieve split, bytes,
        # first/last-used). Gated behind a valid key that carries `admin:api_key`/`admin:audit`
        # (scoped enforced keys); dev/legacy-unrestricted keys read it unchanged.
        if method == "GET" and path == "/v1/admin/api_key_usage":
            allowed, key, tenant, account, key_record = _authorize(scope.get("headers", []), cfg)
            if not allowed:
                return await _json(send, 401, {"error": "unauthorized"})
            denied = _usage_read_denied(key_record)
            if denied is not None:
                return await _json(send, 403, denied)
            usage = _usage_rows_visible_to(key_record, meter.snapshot(), tenant, account)
            return await _json(send, 200, {"status": "ok", "usage": usage, "count": len(usage)})

        # ---- the audit log (auth + admin:audit) ------------------------------------------------
        # The scope catalogue publishes admin:audit as "Read the audit log" and nothing served one.
        # The records existed and the tool that reads them back existed; no route reached it, so the
        # trail was write-only and the scope opened nothing the usage scope did not.
        #
        # The tenant check lives in the tool (ensure_identity_can_manage), and the identity it
        # checks is the one injected here -- the caller's own, never anything they sent.
        if method == "GET" and path == "/v1/admin/audit":
            allowed, key, tenant, account, key_record = _authorize(scope.get("headers", []), cfg)
            if not allowed:
                return await _json(send, 401, {"error": "unauthorized"})
            denied = _audit_read_denied(key_record)
            if denied is not None:
                return await _json(send, 403, denied)
            params = parse_qs(scope.get("query_string", b"").decode("latin-1"))
            try:
                limit = int((params.get("limit") or ["100"])[0])
            except Exception:
                limit = 100
            # The tool reads the whole record log and filters, so the ceiling is on what comes
            # back, not on what it costs to get it. This is a page a person opens, not one that
            # polls.
            limit = min(max(limit, 1), 500)
            args: Json = {"scope": {}, "limit": limit}
            _apply_identity(args, key, tenant, account)
            try:
                result = await asyncio.wait_for(
                    asyncio.to_thread(server.call_tool, "matrixark_admin_audit", args),
                    cfg.backend_timeout)
            except Exception as exc:
                return await _json(send, 502,
                                   _failure(scope, "backend_unavailable", exc))
            rows = (result or {}).get("audit_logs") if isinstance(result, dict) else None
            rows = rows if isinstance(rows, list) else []
            return await _json(send, 200, {
                "status": "ok",
                "audit_logs": rows,
                "count": len(rows),
                # Without this an empty list reads as "nothing to worry about". Off means every
                # record -- including every refusal -- was discarded before it reached storage.
                "recording": _audit_recording_mode(),
            })

        # ---- effective model configuration (auth + admin scope) ------------------------------
        # Which extraction/embedding endpoints this deployment actually talks to, plus the skill
        # budget knobs, so an operator can confirm a deployment from the portal instead of needing
        # shell access. Redacted: reports the NAME of the env var holding each key and whether it
        # resolves, never the key. `warnings` names the silent-fallback cases (see
        # _model_config_snapshot) that otherwise look healthy at the API surface.
        if method == "GET" and path == "/v1/admin/config":
            allowed, key, tenant, account, key_record = _authorize(scope.get("headers", []), cfg)
            if not allowed:
                return await _json(send, 401, {"error": "unauthorized"})
            denied = _usage_read_denied(key_record)
            if denied is not None:
                return await _json(send, 403, denied)
            snapshot = _model_config_snapshot()
            try:
                snapshot["settings"] = _gwconfig.snapshot()
            except Exception as exc:  # never let the write-side registry break the read
                snapshot["settings"] = dict(_failure(scope, "settings_unavailable", exc),
                                                    status="unavailable")
            return await _json(send, 200, snapshot)

        # ---- the deployment chooser (auth + admin scope) --------------------------------------
        # Separate from the settings registry on purpose. The storage directory, the metaserver
        # address and the topology are deliberately NOT writable there -- repointing a running
        # deployment's storage from a browser does not reconfigure it, it strands its data. But
        # "cannot be changed here" and "cannot be chosen at all" are different, and only the first
        # was ever true. Choosing happens when a deployment is launched, so this composes a plan and
        # reports what the engine will actually do with it, without touching this process.
        if method == "GET" and path == "/v1/admin/deployment":
            allowed, key, tenant, account, key_record = _authorize(scope.get("headers", []), cfg)
            if not allowed:
                return await _json(send, 401, {"error": "unauthorized"})
            denied = _usage_read_denied(key_record)
            if denied is not None:
                return await _json(send, 403, denied)
            live = _probe_storage_backend(cfg)
            # Only a live backend of matrixobject PROVES the feature is compiled in. Anything else
            # is silence: auto may have picked another backend on a build that has it. So this
            # reports confirmation, never absence, and the plan preview stays conditional.
            confirmed = bool(live and live.get("backend") == "matrixobject")
            catalogue = _deployment_plan.catalogue(matrixobject_available=True)
            catalogue["matrixobject_confirmed"] = confirmed
            return await _json(send, 200, {
                "status": "ok",
                "catalogue": catalogue,
                "live": live,
                "live_detail": (
                    ("This deployment resolved the %s backend. %s" % (
                        live["backend"],
                        live.get("reason")
                        or "This engine does not publish why it chose it; that is only in its "
                           "startup log.")) if live else
                    "Could not read the datanode's backend. It publishes "
                    "temporalstore_storage_backend on /metrics; an unreachable datanode or a "
                    "gateway pointed at the wrong address both look like this."),
            })

        if method == "POST" and path == "/v1/admin/deployment/plan":
            allowed, key, tenant, account, key_record = _authorize(scope.get("headers", []), cfg)
            if not allowed:
                return await _json(send, 401, {"error": "unauthorized"})
            denied = _usage_read_denied(key_record)
            if denied is not None:
                return await _json(send, 403, denied)
            raw, too_big = await _read_body_capped(receive, 1 << 16)
            if too_big or raw is None:
                return await _json(send, 413, {"error": "body_too_large"})
            try:
                payload = json.loads(raw.decode("utf-8") or "{}")
            except Exception:
                return await _json(send, 400, {"error": "invalid_json"})
            live = _probe_storage_backend(cfg)
            try:
                plan = _deployment_plan.plan(
                    shape=str((payload or {}).get("shape", "")),
                    storage=str((payload or {}).get("storage", "")),
                    nodes=int((payload or {}).get("nodes", 0) or 0),
                    root=str((payload or {}).get("root", "")),
                    shared_dir=str((payload or {}).get("shared_dir", "")),
                    key_envs=list((payload or {}).get("key_envs", []) or []),
                    matrixobject_available=bool(
                        (payload or {}).get("matrixobject_available", True)),
                    configured_engine_settings=_configured_engine_settings(),
                )
            except (TypeError, ValueError) as exc:
                return await _json(send, 400, {"error": "invalid_plan", "detail": str(exc)})
            plan["env_file"] = _deployment_plan.as_env_file(plan)
            plan["live"] = live
            # The launch artifact rides with the plan rather than living behind a second call, so
            # what a customer copies is always derived from the plan they are looking at. A blocked
            # plan gets none: emitting a launch script for a configuration already known not to
            # produce the requested deployment is how the warning gets stepped over.
            if plan.get("ok"):
                try:
                    plan["cloud_init"] = _deployment_plan.cloud_init(plan)
                    plan["commands"] = _deployment_plan.launch_commands(
                        plan,
                        region=str((payload or {}).get("region", "") or "us-east-1"),
                        instance_type=str((payload or {}).get("instance_type", "")
                                          or "m6i.xlarge"),
                        ami=str((payload or {}).get("ami", "")))
                except ValueError as exc:
                    plan["cloud_init"] = ""
                    plan["warnings"] = list(plan.get("warnings") or []) + [str(exc)]
            return await _json(send, 200, plan)

        # ---- write the model configuration (auth + admin scope) ------------------------------
        # The customer-facing half of the same surface: a closed registry of settings (see
        # matrixark_gateway_config.SETTINGS) written to an owner-only file and pushed into the
        # process environment. Only registered keys are accepted, so this is not a general
        # environment-variable setter. The response reports which keys are already in effect and
        # which wait on a restart -- several of these variables are captured into module constants
        # at import, and reporting a write as live when it is not is how a customer ends up
        # convinced they configured DeepSeek while ingest still runs the local rules.
        if method == "POST" and path == "/v1/admin/config":
            allowed, key, tenant, account, key_record = _authorize(scope.get("headers", []), cfg)
            if not allowed:
                return await _json(send, 401, {"error": "unauthorized"})
            denied = _admin_write_denied(key_record)
            if denied is not None:
                return await _json(send, 403, denied)
            raw, too_big = await _read_body_capped(receive, 1 << 18)
            if too_big or raw is None:
                return await _json(send, 413, {"error": "body_too_large"})
            try:
                payload = json.loads(raw.decode("utf-8") or "{}")
            except Exception:
                return await _json(send, 400, {"error": "invalid_json"})
            if not isinstance(payload, dict):
                return await _json(send, 400, {"error": "invalid_json",
                                               "detail": "body must be a JSON object"})
            patch = payload.get("settings") if isinstance(payload.get("settings"), dict) else payload
            try:
                result = _gwconfig.update(patch, actor=account or tenant or "portal")
            except _gwconfig.UnknownSetting as exc:
                return await _json(send, 400, {"error": "unknown_setting", "detail": str(exc)})
            except _gwconfig.InvalidValue as exc:
                return await _json(send, 400, {"error": "invalid_value", "detail": str(exc)})
            except OSError as exc:
                return await _json(send, 500, {"error": "config_write_failed", "detail": str(exc)})
            result["config"] = _model_config_snapshot()
            # A live setting is applied to the environment of THIS worker, and read per call from
            # the environment of whichever worker serves the next request. With more than one, the
            # write is in effect for a fraction of traffic until they are all restarted -- so the
            # answer says how many there are rather than letting the page claim "live now".
            result["workers"] = _worker_count()
            return await _json(send, 200, result)

        # ---- apply a provider preset (auth + admin scope) -------------------------------------
        # A preset only writes values for keys the registry already allows, so it can do nothing a
        # hand-typed write could not. It exists because getting DeepSeek right by hand means knowing
        # that the base URL needs /v1 and that DeepSeek has no embeddings API.
        if method == "POST" and path == "/v1/admin/config/preset":
            allowed, key, tenant, account, key_record = _authorize(scope.get("headers", []), cfg)
            if not allowed:
                return await _json(send, 401, {"error": "unauthorized"})
            denied = _admin_write_denied(key_record)
            if denied is not None:
                return await _json(send, 403, denied)
            raw, too_big = await _read_body_capped(receive, 1 << 16)
            if too_big or raw is None:
                return await _json(send, 413, {"error": "body_too_large"})
            try:
                payload = json.loads(raw.decode("utf-8") or "{}")
            except Exception:
                return await _json(send, 400, {"error": "invalid_json"})
            name = str((payload or {}).get("preset", "")).strip()
            try:
                result = _gwconfig.apply_preset(name, actor=account or tenant or "portal")
            except _gwconfig.UnknownSetting as exc:
                return await _json(send, 400, {"error": "unknown_preset", "detail": str(exc)})
            except OSError as exc:
                return await _json(send, 500, {"error": "config_write_failed", "detail": str(exc)})
            result["config"] = _model_config_snapshot()
            return await _json(send, 200, result)

        # ---- export the configuration as a patch (auth + admin scope) ------------------------
        # Emits exactly what POST /v1/admin/config accepts, so "make staging match production" is
        # one request rather than 79 fields read off one page and retyped into another. Secrets are
        # omitted rather than blanked: a blank is a write that would clear the target's working key.
        if method == "GET" and path == "/v1/admin/config/export":
            allowed, key, tenant, account, key_record = _authorize(scope.get("headers", []), cfg)
            if not allowed:
                return await _json(send, 401, {"error": "unauthorized"})
            denied = _usage_read_denied(key_record)
            if denied is not None:
                return await _json(send, 403, denied)
            params = parse_qs(scope.get("query_string", b"").decode("latin-1"))
            include = (params.get("include_defaults") or [""])[0].strip().lower() in ("1", "true")
            return await _json(send, 200, _gwconfig.export_settings(include_defaults=include))

        # ---- probe the configured model endpoints (auth + admin scope) -----------------------
        # Reading configuration back proves only that it was stored. This calls the endpoints as
        # configured and reports what came back, which is the only way to separate a working key
        # from one that is present and rejected: both look identical in the snapshot, and both
        # degrade to the deterministic path at ingest time with no error the caller ever sees.
        if method == "POST" and path == "/v1/admin/config/test":
            allowed, key, tenant, account, key_record = _authorize(scope.get("headers", []), cfg)
            if not allowed:
                return await _json(send, 401, {"error": "unauthorized"})
            denied = _usage_read_denied(key_record)
            if denied is not None:
                return await _json(send, 403, denied)
            raw, too_big = await _read_body_capped(receive, 1 << 16)
            if too_big:
                return await _json(send, 413, {"error": "body_too_large"})
            try:
                payload = json.loads((raw or b"").decode("utf-8") or "{}")
            except Exception:
                payload = {}
            targets = payload.get("targets") if isinstance(payload.get("targets"), list) else None
            timeout = float(payload.get("timeout_s") or 10.0)
            timeout = min(max(timeout, 1.0), 30.0)
            result = await asyncio.to_thread(_gwconfig.probe, targets, timeout)
            return await _json(send, 200, result)

        # ---- deployment readiness (auth + admin scope) ---------------------------------------
        # One request for the whole setup state. Each item is a thing that fails silently on its
        # own -- an unconfigured provider, an unset ingestion root, an empty store -- so a customer
        # standing the deployment up has no way to notice it short of a checklist like this.
        if method == "GET" and path == "/v1/admin/overview":
            allowed, key, tenant, account, key_record = _authorize(scope.get("headers", []), cfg)
            if not allowed:
                return await _json(send, 401, {"error": "unauthorized"})
            denied = _usage_read_denied(key_record)
            if denied is not None:
                return await _json(send, 403, denied)
            # Three listings, each of which walks the record log. Run them together: done in
            # sequence the page waits for the sum, and this is the page someone leaves open.
            async def _count(label: str, tool: str) -> Tuple[str, Optional[int]]:
                args: Json = {"scope": {}, "limit": 500}
                _apply_identity(args, key, tenant, account)
                try:
                    result = await asyncio.wait_for(
                        asyncio.to_thread(server.call_tool, tool, args), cfg.backend_timeout)
                except Exception:
                    # A backend that cannot answer a listing must not take the whole checklist
                    # down with it -- the config half is exactly what an operator needs when the
                    # backend is the thing that is broken.
                    return label, None
                if isinstance(result, dict) and isinstance(result.get("count"), int):
                    return label, result["count"]
                rows = (result or {}).get(label) if isinstance(result, dict) else None
                return label, (len(rows) if isinstance(rows, list) else None)

            counts: Json = dict(await asyncio.gather(
                _count("skills", "matrixark_list_skills"),
                _count("resources", "matrixark_list_resources"),
                _count("users", "matrixark_list_users"),
            ))
            config_snapshot = _model_config_snapshot()
            try:
                traffic = _gwmetrics.METRICS.snapshot()
            except Exception:
                traffic = {"total_requests": 0}
            imports = _import_progress()
            # Asked once per overview. Best-effort: an unreachable datanode leaves the row
            # saying so rather than failing the page that exists to report on the deployment.
            engine_metrics = _engine_metrics_text(cfg)
            live = _probe_storage_backend(cfg, engine_metrics)
            if live:
                config_snapshot = dict(config_snapshot)
                config_snapshot["live_storage_backend"] = live.get("backend")
                config_snapshot["live_storage_reason"] = live.get("reason") or ""
            # `or ""` and not `or None`: None means nobody has asked, which would send this to ask
            # a second time for a response the line above already has.
            footprint = _footprint_summary(cfg, engine_metrics or "")
            # From the frame's shared cache, so this endpoint adds no probing of its own.
            datanode_state = await _datanode_for_frame(cfg)
            checks = _readiness_checks(config_snapshot, counts, cfg,
                                       float(traffic.get("total_requests") or 0), imports,
                                       datanode=datanode_state)
            done = sum(1 for c in checks if c["status"] == "ok")
            return await _json(send, 200, {
                "status": "ok",
                "checks": checks,
                "done": done,
                "total": len(checks),
                "ready": all(c["status"] != "todo" for c in checks),
                "counts": counts,
                "traffic": traffic,
                "footprint": footprint,
                "imports": imports,
                "config": config_snapshot,
            })

        # ---- model choices (auth + admin scope) -----------------------------------------------
        # A model name typed into a text box is a guess that fails hours later, at ingest, as a
        # silent fall back to the deterministic path. This offers the catalogue, asks the endpoint
        # what it actually serves, and -- for embeddings -- reports what the STORE was written with,
        # because that is what decides whether a change is safe.
        if method == "GET" and path == "/v1/admin/models":
            allowed, key, tenant, account, key_record = _authorize(scope.get("headers", []), cfg)
            if not allowed:
                return await _json(send, 401, {"error": "unauthorized"})
            denied = _usage_read_denied(key_record)
            if denied is not None:
                return await _json(send, 403, denied)
            params = parse_qs(scope.get("query_string", b"").decode("latin-1"))
            target = (params.get("target") or ["extraction"])[0].strip().lower()
            if target not in ("extraction", "embedding"):
                return await _json(send, 400, {
                    "error": "bad_request",
                    "detail": "target must be extraction or embedding",
                })
            discovered = {"available": False, "reason": "not_probed"}
            if (params.get("probe") or ["1"])[0].strip().lower() in ("1", "true", "yes"):
                discovered = await asyncio.to_thread(_gwconfig.discover_models, target)
            body: Json = {"status": "ok", **_model_picker_body(target),
                          "discovered": discovered}
            if target == "embedding":
                body["in_store"] = await _embedding_models_in_store(server, cfg, key, tenant,
                                                                   account)
                body["change_warning"] = _EMBEDDING_CHANGE_WARNING
            return await _json(send, 200, body)

        # ---- per-user settings (auth + admin scope) -------------------------------------------
        # The tenant comes from the KEY, never from the request: a caller must not be able to read
        # or rewrite another tenant's settings by naming it.
        if method in ("GET", "POST") and path == "/v1/admin/policy":
            allowed, key, tenant, account, key_record = _authorize(scope.get("headers", []), cfg)
            if not allowed:
                return await _json(send, 401, {"error": "unauthorized"})
            # One branch serves both methods, so the gate is chosen by method rather than by the
            # branch: the POST below writes user- and tenant-level settings, and read scopes have
            # no business doing that. A combined branch is exactly how this one kept the read gate
            # when the other four writes were moved off it.
            denied = (_admin_write_denied if method == "POST" else _usage_read_denied)(key_record)
            if denied is not None:
                return await _json(send, 403, denied)
            policy_mod = _tenant_policy_module()
            if policy_mod is None:
                return await _json(send, 503, {
                    "error": "policy_unavailable",
                    "detail": "The policy registry could not be loaded in this deployment.",
                })
            tenant_id = str(tenant or "").strip()
            if not tenant_id:
                return await _json(send, 400, {
                    "error": "no_tenant",
                    "detail": "This key is not bound to a tenant, so it has no settings to read.",
                })

            if method == "GET":
                params = parse_qs(scope.get("query_string", b"").decode("latin-1"))
                # Who in THIS tenant has an override at all. The policy endpoints answer for one
                # identity and need its id first, so a tenant could set a user override and then
                # have no way to find it again -- or to answer "who here has custom settings",
                # which is the first question anyone asks after setting the second one.
                #
                # Scoped to the tenant from the key, like everything else on this route. The
                # listing function can answer for every tenant and that form is not served here.
                if (params.get("overrides") or [""])[0].strip() in ("1", "true", "yes"):
                    listing = policy_mod.policy_overrides(only_tenant=tenant_id)
                    return await _json(send, 200, {
                        "tenant": tenant_id,
                        "tenants": listing["tenants"],
                        "users": listing["users"],
                        "user_count": listing["user_count"],
                    })
                user_id = (params.get("user_id") or [""])[0].strip()
                return await _json(send, 200,
                                   _policy_view(policy_mod, tenant_id, user_id))


            raw, too_big = await _read_body_capped(receive, 1 << 20)
            if too_big or raw is None:
                return await _json(send, 413, {"error": "body_too_large"})
            try:
                payload = json.loads(raw.decode("utf-8") or "{}")
            except Exception:
                return await _json(send, 400, {"error": "invalid_json"})
            level = str(payload.get("level") or "user").strip().lower()
            if level not in ("user", "tenant"):
                return await _json(send, 400, {
                    "error": "bad_level",
                    "detail": "level must be user or tenant.",
                })
            user_id = str(payload.get("user_id") or "").strip()
            if level == "user" and not user_id:
                return await _json(send, 400, {
                    "error": "no_user",
                    "detail": "Name the user these settings belong to.",
                })
            settings = payload.get("settings")
            if not isinstance(settings, dict) or not settings:
                return await _json(send, 400, {
                    "error": "no_settings",
                    "detail": "Send a non-empty settings object.",
                })
            asked = sorted(str(name) for name in settings)
            try:
                if level == "tenant":
                    # A tenant owns its whole store, so write-path knobs are accepted here. They
                    # are refused for a single user because two users writing one store under
                    # different rules leave records of two shapes behind.
                    kept = await asyncio.to_thread(
                        policy_mod.set_tenant_policy, tenant_id, settings)
                else:
                    kept = await asyncio.to_thread(
                        policy_mod.set_user_policy, tenant_id, user_id, settings)
            except ValueError as exc:
                return await _json(send, 400, {"error": "bad_request", "detail": str(exc)})
            try:
                if level == "tenant":
                    persisted = await asyncio.to_thread(
                        policy_mod.persist_tenant_policy, tenant_id, settings)
                else:
                    persisted = await asyncio.to_thread(
                        policy_mod.persist_user_policy, tenant_id, user_id, settings)
            except Exception as exc:  # a write failure must not be reported as a save
                persisted = False
                _LOG.warning("%s policy persist failed: %s", level, exc)
            refused = [name for name in asked if name not in kept]
            body = _policy_view(policy_mod, tenant_id, user_id)
            body["level"] = level
            body["applied"] = sorted(kept)
            # Named individually. "Some of your settings were refused" leaves a customer to work
            # out which, and the reason is the same for all of them.
            body["refused"] = refused
            body["persisted"] = bool(persisted)
            if not persisted:
                body["persist_note"] = (
                    "Applied now, but not written anywhere: no policy file is configured "
                    "(MATRIXARK_TENANT_POLICY_PATH), so this is lost when the service restarts.")
            if level == "tenant" and kept:
                # Said once, on the change that has it: a setting can be put back, and the records
                # already written under the old one stay as they are.
                written = [name for name in kept
                           if policy_mod.KNOBS[name].layer == "write"]
                if written:
                    body["already_written_note"] = (
                        "Applies to what happens from now on. Records already stored under the "
                        "previous value keep the shape they were written with — putting the "
                        "setting back does not change them, and re-ingesting is what does: "
                        + ", ".join(sorted(written)) + ".")
            return await _json(send, 200, body)

        # ---- monitoring assets (auth + admin scope) -------------------------------------------
        # The portal used to name a repo path. A customer running this as a managed service has no
        # checkout, and even with one the file on their disk is whatever their copy is rather than
        # what this build emits -- which is the whole failure the dashboard test exists to prevent.
        if method == "GET" and path.startswith("/v1/admin/monitoring/"):
            allowed, key, tenant, account, key_record = _authorize(scope.get("headers", []), cfg)
            if not allowed:
                return await _json(send, 401, {"error": "unauthorized"})
            denied = _usage_read_denied(key_record)
            if denied is not None:
                return await _json(send, 403, denied)
            name = path[len("/v1/admin/monitoring/"):]
            data, content_type = _grafana_asset(name)
            if data is None:
                return await _json(send, 404, {
                    "error": "unknown_asset",
                    "detail": "known assets: " + ", ".join(sorted(_GRAFANA_ASSETS))
                              + ". A deployment that does not ship the docs tree serves none.",
                })
            return await _text(send, 200, data.decode("utf-8"), content_type=content_type)

        # ---- live state (auth + admin scope) --------------------------------------------------
        # One stream in place of three polls per page. Kept before the data routes so it never
        # touches rate limiting or metering -- it is one long request, and counting it as one
        # request against a quota would be as wrong as counting it as thousands.
        if method == "GET" and path == "/v1/admin/events":
            allowed, key, tenant, account, key_record = _authorize(scope.get("headers", []), cfg)
            if not allowed:
                return await _json(send, 401, {"error": "unauthorized"})
            denied = _usage_read_denied(key_record)
            if denied is not None:
                return await _json(send, 403, denied)
            return await _event_stream(server, cfg, scope, receive, send, key, tenant, account)

        # ---- encoding state (auth + admin scope) ---------------------------------------------
        # Ingest can defer encoding: chunking is synchronous and the vector is filled in behind it.
        # Between the write and the drainer catching up a chunk exists and cannot be matched on
        # meaning, so a retrieve over that window returns less than it should and says nothing --
        # which reads as "retrieval is bad" rather than "retrieval is not finished yet".
        if method == "GET" and path == "/v1/admin/embeddings":
            allowed, key, tenant, account, key_record = _authorize(scope.get("headers", []), cfg)
            if not allowed:
                return await _json(send, 401, {"error": "unauthorized"})
            denied = _usage_read_denied(key_record)
            if denied is not None:
                return await _json(send, 403, denied)
            params = parse_qs(scope.get("query_string", b"").decode("latin-1"))
            embed_scope: Json = {}
            for field in ("user_id", "agent_id", "session_id"):
                values = params.get(field)
                if values and values[0]:
                    embed_scope[field] = values[0]
            args: Json = {"scope": embed_scope}
            _apply_identity(args, key, tenant, account)
            try:
                result = await asyncio.wait_for(
                    asyncio.to_thread(server.call_tool, "matrixark_embedding_status", args),
                    cfg.backend_timeout)
            except asyncio.TimeoutError:
                return await _json(send, 504, {"error": "backend_timeout",
                                   "detail": f"backend did not respond within {cfg.backend_timeout}s"})
            except Exception as exc:
                # A backend that cannot answer this must not read as "nothing is pending" -- the
                # honest answer is that the state is unknown.
                return await _json(send, _classify_backend_error(exc),
                                   _failure(scope, "backend_error", exc))
            body = _ok_body(result)
            body["encoder"] = _encoder_summary()
            return await _json(send, 200, body)

        # ---- scope catalogue (auth + admin scope) --------------------------------------------
        # What each scope permits, and four ready-made key shapes. Served rather than hard-coded in
        # the page so the descriptions live next to the map the backend gates with.
        if method == "GET" and path == "/v1/admin/scopes":
            allowed, key, tenant, account, key_record = _authorize(scope.get("headers", []), cfg)
            if not allowed:
                return await _json(send, 401, {"error": "unauthorized"})
            denied = _usage_read_denied(key_record)
            if denied is not None:
                return await _json(send, 403, denied)
            return await _json(send, 200, {"status": "ok", "scopes": SCOPE_CATALOG,
                                           "presets": SCOPE_PRESETS})

        # ---- Prometheus scrape (no auth: counters only, no tenant data) ----------------------
        # The gateway had no /metrics, so the customer-facing API surface was invisible to the
        # dashboards while the engine below it was fully instrumented. These are aggregate
        # ingestion counters -- no keys, no tenant identifiers, nothing per-user -- so the endpoint
        # is safe to scrape without credentials, the way an exporter normally is.
        if method == "GET" and path == "/v1/metrics":
            extra: list[str] = []
            try:
                import matrixark_ingestion_jobs as _jobs
                extra = _jobs.prometheus_text().rstrip(chr(10)).split(chr(10))
            except Exception:
                extra = ["# ingestion job registry unavailable"]
            try:
                config_snapshot = _model_config_snapshot()
            except Exception:
                config_snapshot = None
            try:
                resident = _worker_resident()
                extra.append("# HELP matrixark_gateway_worker_resident_bytes Resident memory of "
                             "the worker that served this scrape.")
                extra.append("# TYPE matrixark_gateway_worker_resident_bytes gauge")
                # Labelled by source so a scrape taken through the fallback is distinguishable
                # from one taken through /proc, rather than silently comparable to it.
                for field, series in (("resident_bytes", "matrixark_gateway_worker_resident_bytes"),
                                      ("peak_bytes", "matrixark_gateway_worker_peak_bytes")):
                    value = resident.get(field)
                    if value is None:
                        continue
                    if series.endswith("peak_bytes"):
                        extra.append("# TYPE matrixark_gateway_worker_peak_bytes gauge")
                    extra.append('%s{source="%s"} %d'
                                 % (series, resident.get("source", "unknown"), int(value)))
                extra.append("matrixark_gateway_workers %d" % _worker_count())
            except Exception:  # pragma: no cover - the scrape is worth more than this figure
                pass
            body = _gwmetrics.prometheus_text(config_snapshot, extra)
            return await _text(send, 200, body, content_type="text/plain; version=0.0.4")

        # ---- ingestion portal page (static HTML, no auth to FETCH) ---------------------------
        # Same posture as the key portal: fetching the page needs nothing, every action on it calls
        # an admin-gated endpoint, so the page is inert without a valid admin key.
        if method == "GET" and path == "/v1/admin/ingestion":
            return await _page(send, scope, _ingestion_portal_html_bytes())

        # ---- ingestion jobs: list (auth + admin scope) ---------------------------------------
        if method == "GET" and path == "/v1/admin/ingestion/jobs":
            allowed, key, tenant, account, key_record = _authorize(scope.get("headers", []), cfg)
            if not allowed:
                return await _json(send, 401, {"error": "unauthorized"})
            denied = _usage_read_denied(key_record)
            if denied is not None:
                return await _json(send, 403, denied)
            import matrixark_ingestion_jobs as _jobs
            return await _json(send, 200, {
                "status": "ok",
                "jobs": _jobs.REGISTRY.list(),
                "ingestion_root": _jobs.ingestion_root(),
            })

        # ---- ingestion jobs: submit (auth + admin scope) -------------------------------------
        # Starts a background import over documents the caller names. Every path is resolved inside
        # MATRIXARK_INGESTION_ROOT (see matrixark_ingestion_jobs) so this cannot be turned into a
        # file-disclosure endpoint; with no root configured, submission is refused rather than
        # defaulting to the whole filesystem.
        if method == "POST" and path == "/v1/admin/ingestion/jobs":
            allowed, key, tenant, account, key_record = _authorize(scope.get("headers", []), cfg)
            if not allowed:
                return await _json(send, 401, {"error": "unauthorized"})
            denied = _admin_write_denied(key_record)
            if denied is not None:
                return await _json(send, 403, denied)
            raw, too_big = await _read_body_capped(receive, 1 << 20)
            if too_big or raw is None:
                return await _json(send, 413, {"error": "body_too_large"})
            try:
                payload = json.loads(raw.decode("utf-8") or "{}")
            except Exception:
                return await _json(send, 400, {"error": "invalid_json"})
            import matrixark_ingestion_jobs as _jobs
            try:
                documents = _jobs.resolve_request_paths(
                    paths=payload.get("paths"),
                    directory=payload.get("directory"),
                    globs=payload.get("globs"),
                )
            except _jobs.IngestionRootNotConfigured as exc:
                return await _json(send, 400, {"error": "ingestion_root_not_configured",
                                               "detail": str(exc)})
            except _jobs.PathOutsideRoot as exc:
                return await _json(send, 403, {"error": "path_outside_ingestion_root",
                                               "detail": str(exc)})
            if not documents:
                return await _json(send, 400, {"error": "no_documents_matched"})
            # A preview resolves and counts the documents without importing them, so a customer can
            # confirm the selection is what they meant before committing to a long run.
            if payload.get("preview"):
                return await _json(send, 200, {
                    "status": "preview",
                    "total": len(documents),
                    "bytes": sum((os.path.getsize(p) if os.path.exists(p) else 0)
                                 for p in documents[:5000]),
                    "sample": documents[:25],
                    "truncated": len(documents) > 25,
                })
            job = _jobs.REGISTRY.submit(documents, {
                "base_url": payload.get("base_url") or ("http://127.0.0.1:%d" % cfg.port
                                                        if getattr(cfg, "port", None) else None),
                "user_id": payload.get("user_id") or "default",
                "api_key_env": payload.get("api_key_env") or "MATRIXARK_API_KEY",
                "timeout_s": payload.get("timeout_s") or 1800.0,
            })
            return await _json(send, 202, job.snapshot())

        # ---- ingestion jobs: submit records (auth + admin scope) -----------------------------
        # The batch a customer pastes or uploads. Unlike the directory import there is no path and
        # no ingestion root to police: the content arrives in the request, so the only limits that
        # matter are the body cap and the record count.
        if method == "POST" and path == "/v1/admin/ingestion/records":
            allowed, key, tenant, account, key_record = _authorize(scope.get("headers", []), cfg)
            if not allowed:
                return await _json(send, 401, {"error": "unauthorized"})
            denied = _admin_write_denied(key_record)
            if denied is not None:
                return await _json(send, 403, denied)
            raw, too_big = await _read_body_capped(receive, MAX_BATCH_RECORDS_BYTES)
            if too_big or raw is None:
                return await _json(send, 413, {
                    "error": "body_too_large",
                    "detail": "A batch is capped at %d bytes. Split it, or import from a "
                              "directory instead." % MAX_BATCH_RECORDS_BYTES,
                })
            try:
                payload = json.loads(raw.decode("utf-8") or "{}")
            except Exception:
                return await _json(send, 400, {"error": "invalid_json"})
            rows = payload.get("records")
            if not isinstance(rows, list) or not rows:
                return await _json(send, 400, {
                    "error": "no_records",
                    "detail": "Send a non-empty records array.",
                })
            if len(rows) > MAX_BATCH_RECORDS:
                return await _json(send, 400, {
                    "error": "too_many_records",
                    "detail": "A batch is capped at %d records; this one has %d."
                              % (MAX_BATCH_RECORDS, len(rows)),
                })
            user_id = str(payload.get("user_id") or "default")
            records: Json = []
            skipped = 0
            for row in rows:
                if not isinstance(row, dict):
                    row = {"text": str(row)}
                text = str(row.get("text") or row.get("content") or "")
                if not text.strip():
                    # Dropped here rather than queued to fail one at a time: an empty record fails
                    # identically every time, so letting it into the job would fill the failure
                    # list with entries no retry can ever clear.
                    skipped += 1
                    continue
                entry = {"text": text,
                         "user_id": str(row.get("user_id") or user_id)}
                for name in ("agent_id", "session_id", "identity_key", "role"):
                    value = str(row.get(name) or payload.get(name) or "")
                    if value:
                        entry[name] = value
                if isinstance(row.get("metadata"), dict) and row["metadata"]:
                    entry["metadata"] = row["metadata"]
                records.append(entry)
            if not records:
                return await _json(send, 400, {
                    "error": "no_usable_records",
                    "detail": "Every record was empty. %d skipped." % skipped,
                })
            import matrixark_ingestion_jobs as _jobs
            if payload.get("preview"):
                return await _json(send, 200, {
                    "status": "preview",
                    "total": len(records),
                    "skipped": skipped,
                    "user_id": user_id,
                    "sample": records[:10],
                    "truncated": len(records) > 10,
                })
            job = _jobs.REGISTRY.submit(_jobs.record_items(records), {
                "base_url": payload.get("base_url") or ("http://127.0.0.1:%d" % cfg.port
                                                        if getattr(cfg, "port", None) else None),
                "user_id": user_id,
                "api_key_env": payload.get("api_key_env") or "MATRIXARK_API_KEY",
                "timeout_s": payload.get("timeout_s") or 1800.0,
            })
            snapshot = job.snapshot()
            snapshot["skipped"] = skipped
            return await _json(send, 202, snapshot)

        # ---- retry a job's failed documents (auth + admin scope) -----------------------------
        # Only the failures, and by default only the ones worth retrying. Ingest is a keyed upsert
        # so re-running everything is safe -- which is why it is the tempting thing to do, and why
        # a thousand-document import with three failures gets re-run in full. A 4xx will fail again
        # identically; a timeout very likely will not.
        if method == "POST" and path.startswith("/v1/admin/ingestion/jobs/") \
                and path.endswith("/retry"):
            allowed, key, tenant, account, key_record = _authorize(scope.get("headers", []), cfg)
            if not allowed:
                return await _json(send, 401, {"error": "unauthorized"})
            denied = _usage_read_denied(key_record)
            if denied is not None:
                return await _json(send, 403, denied)
            raw, too_big = await _read_body_capped(receive, 1 << 16)
            if too_big:
                return await _json(send, 413, {"error": "body_too_large"})
            try:
                payload = json.loads((raw or b"").decode("utf-8") or "{}")
            except Exception:
                payload = {}
            only_retryable = payload.get("only_retryable")
            only_retryable = True if only_retryable is None else bool(only_retryable)
            import matrixark_ingestion_jobs as _jobs
            job_id = path[len("/v1/admin/ingestion/jobs/"):-len("/retry")]
            parent = _jobs.REGISTRY.get(job_id)
            if parent is None:
                return await _json(send, 404, {"error": "unknown_job", "job_id": job_id})
            if parent.snapshot()["state"] == "running":
                return await _json(send, 409, {
                    "error": "job_still_running",
                    "detail": "Wait for the import to finish, or cancel it first — retrying while "
                              "it is still working would submit documents it is about to retry "
                              "itself.",
                })
            child = _jobs.REGISTRY.retry(job_id, only_retryable=only_retryable)
            if child is None:
                return await _json(send, 400, {
                    "error": "nothing_to_retry",
                    "detail": ("No retryable failures. Failures that are 4xx are the request's "
                               "own fault and will fail again identically; pass "
                               "only_retryable=false to resubmit them anyway.")
                    if parent.snapshot()["failed"] else "This job had no failures.",
                })
            return await _json(send, 202, child.snapshot())

        # ---- cancel a running ingestion job (auth + admin scope) -----------------------------
        # Stops the job before its next document; documents already imported stay imported, which
        # is safe because ingest is a keyed upsert and a later re-run replaces rather than duplicates.
        if method == "POST" and path.startswith("/v1/admin/ingestion/jobs/") and path.endswith("/cancel"):
            allowed, key, tenant, account, key_record = _authorize(scope.get("headers", []), cfg)
            if not allowed:
                return await _json(send, 401, {"error": "unauthorized"})
            denied = _usage_read_denied(key_record)
            if denied is not None:
                return await _json(send, 403, denied)
            import matrixark_ingestion_jobs as _jobs
            job_id = path[len("/v1/admin/ingestion/jobs/"):-len("/cancel")]
            job = _jobs.REGISTRY.get(job_id)
            if job is None:
                return await _json(send, 404, {"error": "unknown_job", "job_id": job_id})
            job.cancel()
            return await _json(send, 202, job.snapshot())

        # ---- get_all via GET /v1/memories (auth + context:retrieve) -------------------------
        # Convenience read: list a scope's active memories. Scope identity comes from the query
        # string (user_id/agent_id/session_id); tenant is pinned from the authenticated key via
        # _apply_identity (same isolation as every other data route). POST /v1/memories with a JSON
        # body is also supported through the data-route dispatch below.
        if method == "GET" and path == "/v1/memories":
            allowed, key, tenant, account, key_record = _authorize(scope.get("headers", []), cfg)
            if not allowed:
                return await _json(send, 401, {"error": "unauthorized"})
            denied = _scope_denied(key_record, "context:retrieve")
            if denied is not None:
                return await _json(send, 403, denied)
            params = parse_qs(scope.get("query_string", b"").decode("latin-1"))

            def _q(name: str) -> Optional[str]:
                values = params.get(name)
                return values[0] if values else None

            query_scope: Json = {}
            for field in ("user_id", "agent_id", "session_id"):
                value = _q(field)
                if value:
                    query_scope[field] = value
            args: Json = {"scope": query_scope}
            limit = _q("limit")
            if limit and limit.strip().lstrip("-").isdigit():
                args["limit"] = int(limit)
            _apply_identity(args, key, tenant, account)
            denied = _identity_denied(key_record, args)
            if denied is not None:
                return await _json(send, 403, denied)
            try:
                result = await asyncio.wait_for(
                    asyncio.to_thread(server.call_tool, "matrixark_get_all", args), cfg.backend_timeout)
            except asyncio.TimeoutError:
                return await _json(send, 504, {"error": "backend_timeout",
                                   "detail": f"backend did not respond within {cfg.backend_timeout}s"})
            except Exception as exc:
                return await _json(send, _classify_backend_error(exc),
                                   _failure(scope, "backend_error", exc))
            return await _json(send, 200, _ok_body(result))

        # ---- who holds memories: GET /v1/users (auth + context:retrieve) ---------------------
        # POST /v1/users has always worked through the data-route dispatch; this is the GET form,
        # matching GET /v1/memories, so the same read is reachable from a browser or a plain curl.
        if method == "GET" and path == "/v1/users":
            allowed, key, tenant, account, key_record = _authorize(scope.get("headers", []), cfg)
            if not allowed:
                return await _json(send, 401, {"error": "unauthorized"})
            denied = _scope_denied(key_record, "context:retrieve")
            if denied is not None:
                return await _json(send, 403, denied)
            params = parse_qs(scope.get("query_string", b"").decode("latin-1"))
            user_scope: Json = {}
            for field in ("user_id", "agent_id", "session_id"):
                values = params.get(field)
                if values and values[0]:
                    user_scope[field] = values[0]
            args: Json = {"scope": user_scope}
            limits = params.get("limit")
            if limits and limits[0].strip().isdigit():
                args["limit"] = min(int(limits[0]), 500)
            _apply_identity(args, key, tenant, account)
            denied = _identity_denied(key_record, args)
            if denied is not None:
                return await _json(send, 403, denied)
            try:
                result = await asyncio.wait_for(
                    asyncio.to_thread(server.call_tool, "matrixark_list_users", args),
                    cfg.backend_timeout)
            except asyncio.TimeoutError:
                return await _json(send, 504, {"error": "backend_timeout",
                                   "detail": f"backend did not respond within {cfg.backend_timeout}s"})
            except Exception as exc:
                return await _json(send, _classify_backend_error(exc),
                                   _failure(scope, "backend_error", exc))
            return await _json(send, 200, _ok_body(result))

        # ---- skill / resource catalog (auth + resource:read / skill:read) --------------------
        # The backend has had matrixark_list_skills / matrixark_list_resources since the skill lane
        # landed, but they were reachable only through /v1/mcp -- so a customer on the documented
        # REST contract could ingest skills and had no way to see what was stored. These are the two
        # missing reads, gated on the same per-tool scopes the backend enforces.
        if method == "GET" and path in ("/v1/skills", "/v1/resources"):
            allowed, key, tenant, account, key_record = _authorize(scope.get("headers", []), cfg)
            if not allowed:
                return await _json(send, 401, {"error": "unauthorized"})
            is_skills = path == "/v1/skills"
            denied = _scope_denied(key_record, "skill:read" if is_skills else "resource:read")
            if denied is not None:
                return await _json(send, 403, denied)
            params = parse_qs(scope.get("query_string", b"").decode("latin-1"))

            def _qc(name: str) -> Optional[str]:
                values = params.get(name)
                return values[0] if values else None

            catalog_scope: Json = {}
            for field in ("user_id", "agent_id", "session_id"):
                value = _qc(field)
                if value:
                    catalog_scope[field] = value
            args: Json = {"scope": catalog_scope}
            limit = _qc("limit")
            if limit and limit.strip().isdigit():
                args["limit"] = min(int(limit), 500)
            if is_skills:
                if str(_qc("include_disabled") or "").strip().lower() in ("1", "true", "yes"):
                    args["include_disabled"] = True
            else:
                resource_type = _qc("resource_type")
                if resource_type:
                    args["resource_type"] = resource_type
            _apply_identity(args, key, tenant, account)
            denied = _identity_denied(key_record, args)
            if denied is not None:
                return await _json(send, 403, denied)
            tool = "matrixark_list_skills" if is_skills else "matrixark_list_resources"
            try:
                result = await asyncio.wait_for(
                    asyncio.to_thread(server.call_tool, tool, args), cfg.backend_timeout)
            except asyncio.TimeoutError:
                return await _json(send, 504, {"error": "backend_timeout",
                                   "detail": f"backend did not respond within {cfg.backend_timeout}s"})
            except Exception as exc:
                return await _json(send, _classify_backend_error(exc),
                                   _failure(scope, "backend_error", exc))
            return await _json(send, 200, _ok_body(result))

        # ---- enable / disable a skill (auth + skill:manage) ----------------------------------
        # Listing skills without being able to retire one leaves a customer with a catalog that
        # only grows: a superseded playbook keeps competing for pack slots with its replacement,
        # and the only way to stop it was to edit the registry out of band.
        if method == "POST" and path == "/v1/skills/update":
            allowed, key, tenant, account, key_record = _authorize(scope.get("headers", []), cfg)
            if not allowed:
                return await _json(send, 401, {"error": "unauthorized"})
            denied = _scope_denied(key_record, "skill:manage")
            if denied is not None:
                return await _json(send, 403, denied)
            raw, too_big = await _read_body_capped(receive, 1 << 18)
            if too_big or raw is None:
                return await _json(send, 413, {"error": "body_too_large"})
            try:
                payload = json.loads(raw.decode("utf-8") or "{}")
            except Exception:
                return await _json(send, 400, {"error": "invalid_json"})
            if not isinstance(payload, dict) or not isinstance(payload.get("skill_hash"), int):
                return await _json(send, 400, {"error": "bad_request",
                                               "detail": "skill_hash (integer) is required"})
            args = {k: v for k, v in payload.items()
                    if k in ("skill_hash", "status", "precedence", "owner_scope", "version",
                             "triggers", "allowed_tools", "scope")}
            args.setdefault("scope", {})
            _apply_identity(args, key, tenant, account)
            denied = _identity_denied(key_record, args)
            if denied is not None:
                return await _json(send, 403, denied)
            try:
                result = await asyncio.wait_for(
                    asyncio.to_thread(server.call_tool, "matrixark_update_skill", args),
                    cfg.backend_timeout)
            except asyncio.TimeoutError:
                return await _json(send, 504, {"error": "backend_timeout",
                                   "detail": f"backend did not respond within {cfg.backend_timeout}s"})
            except Exception as exc:
                return await _json(send, _classify_backend_error(exc),
                                   _failure(scope, "backend_error", exc))
            return await _json(send, 200, _ok_body(result))

        # ---- keyed recall via GET /v1/memory/by-key?identity_key=... (auth + context:retrieve) --
        # PurchaseMemory keyed-upsert recall: the single current live value for an identity_key in a
        # scope. Must precede the /v1/memory/<id> branch below so "by-key" is not treated as an id.
        if method == "GET" and path == "/v1/memory/by-key":
            allowed, key, tenant, account, key_record = _authorize(scope.get("headers", []), cfg)
            if not allowed:
                return await _json(send, 401, {"error": "unauthorized"})
            denied = _scope_denied(key_record, "context:retrieve")
            if denied is not None:
                return await _json(send, 403, denied)
            params = parse_qs(scope.get("query_string", b"").decode("latin-1"))

            def _qk(name: str) -> Optional[str]:
                values = params.get(name)
                return values[0] if values else None

            identity_key = _qk("identity_key")
            if not identity_key:
                return await _json(send, 400, {"error": "bad_request", "detail": "identity_key query param is required"})
            query_scope: Json = {}
            for field in ("user_id", "agent_id", "session_id"):
                value = _qk(field)
                if value:
                    query_scope[field] = value
            args = {"identity_key": identity_key, "scope": query_scope}
            _apply_identity(args, key, tenant, account)
            denied = _identity_denied(key_record, args)
            if denied is not None:
                return await _json(send, 403, denied)
            try:
                result = await asyncio.wait_for(
                    asyncio.to_thread(server.call_tool, "matrixark_get_memory_by_key", args), cfg.backend_timeout)
            except asyncio.TimeoutError:
                return await _json(send, 504, {"error": "backend_timeout",
                                   "detail": f"backend did not respond within {cfg.backend_timeout}s"})
            except Exception as exc:
                return await _json(send, _classify_backend_error(exc),
                                   _failure(scope, "backend_error", exc))
            if result.get("found") is False:
                return await _json(send, 404, _ok_body(result))
            return await _json(send, 200, _ok_body(result))

        # ---- get / history via GET /v1/memory/<id>[/history] (auth + context:retrieve) -------
        # Single-memory read (mem0 get) and change history (mem0 history). The memory id is in the
        # path; tenant is pinned from the authenticated key (same isolation as every data route). The
        # id-scoped tools reconstruct the memory's own scope from its stored record.
        if method == "GET" and path.startswith("/v1/memory/") and path != "/v1/memories":
            allowed, key, tenant, account, key_record = _authorize(scope.get("headers", []), cfg)
            if not allowed:
                return await _json(send, 401, {"error": "unauthorized"})
            denied = _scope_denied(key_record, "context:retrieve")
            if denied is not None:
                return await _json(send, 403, denied)
            remainder = path[len("/v1/memory/"):]
            if remainder.endswith("/history"):
                memory_id = remainder[: -len("/history")]
                tool = "matrixark_memory_history"
            else:
                memory_id = remainder
                tool = "matrixark_get_memory"
            memory_id = unquote(memory_id).strip("/")
            if not memory_id:
                return await _json(send, 404, {"error": "not_found"})
            args = {"memory_id": memory_id, "scope": {}}
            _apply_identity(args, key, tenant, account)
            try:
                result = await asyncio.wait_for(
                    asyncio.to_thread(server.call_tool, tool, args), cfg.backend_timeout)
            except asyncio.TimeoutError:
                return await _json(send, 504, {"error": "backend_timeout",
                                   "detail": f"backend did not respond within {cfg.backend_timeout}s"})
            except Exception as exc:
                return await _json(send, _classify_backend_error(exc),
                                   _failure(scope, "backend_error", exc))
            if tool == "matrixark_get_memory" and result.get("found") is False:
                return await _json(send, 404, _ok_body(result))
            return await _json(send, 200, _ok_body(result))

        # ---- blob (auth + concurrent-stream cap, streamed) ----------------------------------
        if path.startswith("/v1/blob/"):
            allowed, key, tenant, account, key_record = _authorize(scope.get("headers", []), cfg)
            if not allowed:
                return await _json(send, 401, {"error": "unauthorized"})
            denied = _scope_denied(key_record, _required_scope(path, method, None))
            if denied is not None:
                return await _json(send, 403, denied)
            quota = _meter_and_check_quota(meter, cfg, key, key_record, tenant, account,
                                           "retrieve" if method == "GET" else "ingest")
            if quota is not None:
                return await _json(send, 429, quota[0], quota[1])
            key_str = path[len("/v1/blob/"):]
            if not key_str:
                return await _json(send, 404, {"error": "not_found"})
            if not limiter.blob_acquire():
                return await _json(send, 429, {"error": "rate_limited"}, [(b"retry-after", b"1")])
            try:
                if method in ("PUT", "POST"):
                    return await _blob_put(scope, receive, send, method, cfg, key_str, tenant, account)
                if method == "GET":
                    return await _blob_get(send, cfg, key_str, tenant, account)
                return await _json(send, 405, {"error": "method_not_allowed"})
            finally:
                limiter.blob_release()

        # ---- combined upload-and-ingest (auth + concurrent-stream cap, streamed) -------------
        if path == "/v1/ingest_file":
            allowed, key, tenant, account, key_record = _authorize(scope.get("headers", []), cfg)
            if not allowed:
                return await _json(send, 401, {"error": "unauthorized"})
            denied = _scope_denied(key_record, _required_scope(path, method, None))
            if denied is not None:
                return await _json(send, 403, denied)
            if method != "POST":
                return await _json(send, 405, {"error": "method_not_allowed"})
            if not limiter.blob_acquire():
                return await _json(send, 429, {"error": "rate_limited"}, [(b"retry-after", b"1")])
            try:
                quota = _meter_and_check_quota(meter, cfg, key, key_record, tenant, account, "ingest")
                if quota is not None:
                    return await _json(send, 429, quota[0], quota[1])
                return await _ingest_file(app, scope, receive, send, cfg, key, tenant, account)
            finally:
                limiter.blob_release()

        # ---- data routes --------------------------------------------------------------------
        route = _DATA_ROUTES.get(path)
        if route is None:
            return await _json(send, 404, {"error": "not_found"})
        tool, cls = route
        if method != "POST":
            return await _json(send, 405, {"error": "method_not_allowed"})

        allowed, key, tenant, account, key_record = _authorize(scope.get("headers", []), cfg)
        if not allowed:
            return await _json(send, 401, {"error": "unauthorized"})
        # /v1/mcp is gated PER-TOOL after the JSON-RPC body is parsed (see the `__mcp__` branch
        # below), not at this coarse route level -- the route's blanket `context:retrieve` scope
        # can't distinguish a data `tools/call` from an admin one. Every other data route keeps its
        # single route->scope gate here.
        if tool != "__mcp__":
            denied = _scope_denied(key_record, _required_scope(path, method, route))
            if denied is not None:
                return await _json(send, 403, denied)

        ok, remaining, reset, retry = limiter.check(key or "anon", cls)
        rl_headers = limiter.headers(cls, remaining, reset)
        if not ok:
            headers = rl_headers + [(b"retry-after", str(int(math.ceil(retry))).encode())]
            return await _json(send, 429, {"error": "rate_limited"}, headers)

        raw, too_big = await _read_body_capped(receive, cfg.max_body_bytes)
        if too_big:
            return await _json(send, 413, {"error": "payload_too_large"}, rl_headers)
        try:
            parsed = json.loads(raw or b"{}")
            if not isinstance(parsed, dict):
                raise ValueError("body must be a JSON object")
        except (json.JSONDecodeError, ValueError) as exc:
            return await _json(send, 400, {"error": "bad_request", "detail": str(exc)}, rl_headers)

        # Meter the authenticated request (best-effort, off the response path) and enforce the key's
        # request_quota. `cls` is the ingest/retrieve category; `raw` is the already-buffered body,
        # so bytes are free here. A key with no quota is never limited; enforcement is O(1) against
        # the meter's counter and best-effort (a quota-check bug can never block/crash the request).
        quota = _meter_and_check_quota(meter, cfg, key, key_record, tenant, account, cls, len(raw or b""))
        if quota is not None:
            return await _json(send, 429, quota[0], rl_headers + quota[1])

        # MCP-over-HTTP: dispatch the JSON-RPC message directly (api-key injected downstream).
        if tool == "__mcp__":
            # PER-TOOL edge gate: the body is already buffered + parsed ONCE above (`parsed`), so we
            # inspect `method` / `params.name` / `params.arguments.scope` here and forward the SAME
            # `parsed` object to mcp_http_dispatch untouched -- the ASGI receive stream is never read
            # twice. Enforced-mode scoped keys get per-tool scope + user/session checks; dev/legacy
            # keys are unrestricted (see `_mcp_denied`), so /v1/mcp stays byte-identical for them.
            denied = _mcp_denied(key_record, parsed)
            if denied is not None:
                return await _json(send, 403, denied, rl_headers)
            try:
                resp = await asyncio.wait_for(
                    asyncio.to_thread(mcp_http_dispatch, server, parsed, api_key=key), cfg.backend_timeout)
            except asyncio.TimeoutError:
                return await _json(send, 504, {"error": "backend_timeout",
                                   "detail": f"backend did not respond within {cfg.backend_timeout}s"}, rl_headers)
            except Exception as exc:
                return await _json(send, _classify_backend_error(exc),
                                   _failure(scope, "backend_error", exc), rl_headers)
            return await _json(send, 200, resp, rl_headers)

        args = parsed.get("arguments") if isinstance(parsed.get("arguments"), dict) else parsed
        if not isinstance(args, dict):
            args = {}

        # ---- optional PurchaseMemory TTL headers on /v1/ingest -------------------------------
        # X-Expires-At (absolute unix seconds) / X-Ttl-Seconds (relative) are a header-form of the
        # JSON body fields; the JSON body always wins when both are present.
        if tool == "matrixark_ingest":
            hmap = _headers_map(scope)
            header_expires_at = hmap.get("x-expires-at")
            if header_expires_at and "expires_at" not in args:
                try:
                    args["expires_at"] = float(header_expires_at)
                except (TypeError, ValueError):
                    pass
            header_ttl = hmap.get("x-ttl-seconds")
            if header_ttl and "ttl_seconds" not in args:
                try:
                    args["ttl_seconds"] = float(header_ttl)
                except (TypeError, ValueError):
                    pass

        records = args.get("records")
        messages = args.get("messages")
        n_records = len(records) if isinstance(records, list) else None
        n_messages = len(messages) if isinstance(messages, list) else None
        batch = n_records if n_records is not None else (n_messages or 0)
        if batch > cfg.max_batch:
            return await _json(send, 413, {"error": "payload_too_large", "detail": "batch too large"}, rl_headers)

        # ---- direct network path (B): bypass the serial MCP adapter --------------------------
        # /v1/ingest and /v1/retrieve POST a high-level {scope, messages}/{scope, query} body over
        # the shared pool to the Rust service proxy, which owns tenant_hash + shard routing. When
        # the flag is off, control falls through to the byte-for-byte-unchanged MCP path below.
        if direct_client is not None and tool in (
                "matrixark_ingest", "matrixark_retrieve", "matrixark_session_commit"):
            _apply_identity(args, key, tenant, account)  # scope object only; proxy does the hashing
            denied = _identity_denied(key_record, args)
            if denied is not None:
                return await _json(send, 403, denied, rl_headers)
            return await _dispatch_direct(
                direct_client, cfg, tool, parsed, args, send, rl_headers,
                n_records=n_records, n_messages=n_messages)

        apply_ingest_route_defaults("/api/ingest" if tool == "matrixark_ingest" else path, args)
        # A plain (non-finalize) streaming ingest schedules a backend-native idle-commit task so
        # the gateway's background materializer can drain it and make the ingest retrievable on
        # its own -- without waiting for a client retrieve. A finalize ingest extracts inline
        # below, so it is left alone. Callers keep control by passing idle_commit_timeout_ms.
        if (
            tool == "matrixark_ingest"
            and not _finalize_requested(parsed, args)
            and cfg.stream_idle_commit_timeout_ms > 0
        ):
            args.setdefault("idle_commit_timeout_ms", cfg.stream_idle_commit_timeout_ms)
        _apply_identity(args, key, tenant, account)
        denied = _identity_denied(key_record, args)
        if denied is not None:
            return await _json(send, 403, denied, rl_headers)

        try:
            result = await asyncio.wait_for(
                asyncio.to_thread(server.call_tool, tool, args), cfg.backend_timeout)
        except asyncio.TimeoutError:
            return await _json(send, 504, {"error": "backend_timeout",
                               "detail": f"backend did not respond within {cfg.backend_timeout}s"}, rl_headers)
        except Exception as exc:
            status = _classify_backend_error(exc)
            body = {"error": "rate_limited"} if status == 429 else (
                _failure(scope, "storage_quota_exceeded", exc) if status == 507
                else _failure(scope, "backend_error", exc))
            headers = rl_headers + ([(b"retry-after", b"1")] if status == 429 else [])
            return await _json(send, status, body, headers)

        if tool == "matrixark_ingest":
            accepted = n_records if n_records is not None else (n_messages or 0)
            out = {"accepted": accepted, "scope": args.get("scope"), "result": result}
            # A complete conversation (finalize/commit/kind=conversation) triggers the batched
            # extraction NOW; a plain streaming message buffers and extracts later on commit/timeout.
            if _finalize_requested(parsed, args):
                commit_args = {"scope": args.get("scope")}
                _apply_identity(commit_args, key, tenant, account)
                try:
                    out["extraction"] = await asyncio.wait_for(
                        asyncio.to_thread(server.call_tool, "matrixark_session_commit", commit_args),
                        cfg.backend_timeout)
                    out["finalized"] = True
                except asyncio.TimeoutError:
                    out["finalized"] = False
                    out["extraction_error"] = "backend_timeout"
                except Exception as exc:  # extraction failure must NOT lose the durable ingest
                    out["finalized"] = False
                    out["extraction_error"] = "extraction_failed"
                    out["extraction_incident"] = _incident(scope, "extraction_failed", exc)
            elif args.get("idle_commit_timeout_ms"):
                # Plain streaming ingest: register the scope so the gateway's background
                # materializer drains its scheduled idle-commit after the debounce, making the
                # ingest retrievable without a client retrieve. Best-effort: the durable
                # scheduled-task record + retrieve-time flush remain the backstop.
                register = getattr(server, "register_stream_materialize_scope", None)
                if callable(register):
                    try:
                        register(args.get("scope"), int(time.time() * 1000) + int(args.get("idle_commit_timeout_ms") or 0))
                    except Exception:
                        pass
            return await _json(send, 202, out, rl_headers)
        return await _json(send, 200, _ok_body(result), rl_headers)

    async def app(scope: Json, receive: Callable, send: Callable) -> None:
        """Record one edge observation per HTTP request, then serve it.

        Wrapping here rather than instrumenting each branch means a route added later is measured
        for free, and a route that returns early (401, 429, 404) is measured too -- which is exactly
        the traffic an operator most needs to see and the traffic a per-branch counter always
        misses. Recording never raises: a metrics failure must not be able to fail a request.
        """
        if scope.get("type") != "http":
            return await _serve(scope, receive, send)
        started = time.time()
        observed = {"status": 0, "response_bytes": 0, "request_bytes": 0}

        async def _observed_receive() -> Json:
            message = await receive()
            if message.get("type") == "http.request":
                observed["request_bytes"] += len(message.get("body") or b"")
            return message

        async def _observed_send(message: Json) -> None:
            mtype = message.get("type")
            if mtype == "http.response.start":
                try:
                    observed["status"] = int(message.get("status") or 0)
                except (TypeError, ValueError):
                    observed["status"] = 0
            elif mtype == "http.response.body":
                observed["response_bytes"] += len(message.get("body") or b"")
            await send(message)

        _gwmetrics.METRICS.begin()
        try:
            return await _serve(scope, _observed_receive, _observed_send)
        finally:
            _gwmetrics.METRICS.end()
            try:
                _gwmetrics.METRICS.record(
                    scope.get("path", ""), scope.get("method", ""), observed["status"],
                    time.time() - started,
                    request_bytes=observed["request_bytes"],
                    response_bytes=observed["response_bytes"],
                    incident=_INCIDENT.get(""))
            except Exception:  # pragma: no cover - metrics must never break a response
                pass

    return app


# ================================================================================================
# Entrypoints
# ================================================================================================
def _build_server_from_env() -> Any:
    import argparse

    try:
        from tools.matrixark_mcp_server import MatrixArkMcpServer
        from tools.matrixark_mcp_backends import (
            add_backend_arguments,
            build_mcp_adapter,
            default_mcp_backend,
        )
    except ImportError:
        from matrixark_mcp_server import MatrixArkMcpServer  # type: ignore
        from matrixark_mcp_backends import (  # type: ignore
            add_backend_arguments,
            build_mcp_adapter,
            default_mcp_backend,
        )
    # Build the *full* backend namespace (argparse defaults + env), then force the backend.
    # Previously only `backend` was set on the Namespace, so build_mcp_adapter() reached for
    # `args.event_log` / `args.local_store` / `args.metaserver` / ... and raised AttributeError
    # for every backend -> the gateway could never build its server. parse_args([]) gives every
    # backend arg its documented default (event_log=/tmp/matrixark-mcp-events.jsonl, etc.).
    parser = argparse.ArgumentParser(add_help=False)
    add_backend_arguments(parser)
    ns = parser.parse_args([])
    ns.backend = os.environ.get("MATRIXARK_MCP_BACKEND", default_mcp_backend())
    event_log_override = os.environ.get("MATRIXARK_EVENT_LOG")
    if event_log_override:
        from pathlib import Path

        ns.event_log = Path(event_log_override)
    adapter = build_mcp_adapter(ns)
    # DEV DEFAULT: access_mode defaults to "dev" (anonymous allowed) so the server
    # works out of the box; set MATRIXARK_ACCESS_MODE=enforced in production.
    return MatrixArkMcpServer(adapter, access_mode=os.environ.get("MATRIXARK_ACCESS_MODE", "dev"))


def create_v1_app() -> Callable[..., Awaitable[None]]:
    """Build the MatrixArk server from env and return the `/v1/*` gateway ASGI app.

    Mirrors `matrixark_asgi.create_app` for `uvicorn matrixark_v1_gateway:application`.
    """
    server = _build_server_from_env()
    app = make_v1_app(server, GatewayConfig.from_env())
    # Eager per-process start so the background materializer runs even under ASGI servers that
    # never drive the lifespan protocol. Idempotent with the lifespan.startup start above.
    ensure = getattr(server, "ensure_stream_materialize_worker", None)
    if callable(ensure):
        try:
            ensure()
        except Exception:
            pass
    return app


_LAZY_APP: Optional[Callable[..., Awaitable[None]]] = None


async def application(scope: Json, receive: Callable, send: Callable) -> None:
    """Lazy module-level ASGI export: builds the real app on first request (import stays cheap)."""
    global _LAZY_APP
    if _LAZY_APP is None:
        _LAZY_APP = create_v1_app()
    return await _LAZY_APP(scope, receive, send)


def main() -> int:
    try:
        import uvicorn
    except ImportError:
        raise SystemExit(
            "uvicorn is not installed. The Cloud API gateway runs under an ASGI server:\n"
            "    pip install uvicorn && uvicorn matrixark_v1_gateway:application --workers 4"
        )
    uvicorn.run(
        create_v1_app(),
        host=os.environ.get("MATRIXARK_HTTP_HOST", "0.0.0.0"),
        port=int(os.environ.get("MATRIXARK_HTTP_PORT", "8080")),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
