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
import hashlib
import json
import logging
import math
import os
import sys
import tempfile
import threading
import time
from typing import Any, Awaitable, Callable, Optional, Tuple
from urllib.parse import parse_qs, unquote, urlparse

try:
    from tools.matrixark_asgi import make_asgi_app, _api_key
    from tools.matrixark_http import apply_ingest_route_defaults, mcp_http_dispatch
except ImportError:  # Direct script execution from tools/.
    from matrixark_asgi import make_asgi_app, _api_key  # type: ignore
    from matrixark_http import apply_ingest_route_defaults, mcp_http_dispatch  # type: ignore

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

_NO_AUTH_WARNING = (
    "MatrixArk gateway is running WITHOUT authentication (dev default). Anyone who "
    "can reach this address has full anonymous access and there is NO tenant "
    "isolation. Set MATRIXARK_REQUIRE_AUTH=1 (and MATRIXARK_ACCESS_MODE=enforced) "
    "to enforce API keys."
)


def _warn_if_auth_disabled(cfg: "GatewayConfig") -> None:
    """Emit a one-time, NON-BLOCKING warning when auth is effectively off
    (``require_auth`` False). Never rejects requests or blocks startup — behavior
    stays fully anonymous-allowed; this only surfaces the posture to the operator."""
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
async def _json(send: Callable, status: int, payload: Json,
                extra_headers: Optional[list[Tuple[bytes, bytes]]] = None) -> None:
    data = json.dumps(payload).encode("utf-8")
    headers = [(b"content-type", b"application/json"), (b"content-length", str(len(data)).encode())]
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


async def _html(send: Callable, status: int, body: bytes,
                extra_headers: Optional[list[Tuple[bytes, bytes]]] = None) -> None:
    headers = [(b"content-type", b"text/html; charset=utf-8"),
               (b"content-length", str(len(body)).encode())]
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
    extraction_key_env = _env("MATRIXARK_EXTRACTION_API_KEY_ENV", "OPENAI_API_KEY")
    embedding_provider = _env("MATRIXARK_EMBEDDING_PROVIDER", "deterministic")
    embedding_key_env = _env("MATRIXARK_EMBEDDING_API_KEY_ENV", "OPENAI_API_KEY")
    require_model_embeddings = _env("MATRIXARK_REQUIRE_MODEL_EMBEDDINGS") in {"1", "true", "yes", "on"}

    extraction: Json = {
        "provider": extraction_provider,
        "base_url": _env("MATRIXARK_EXTRACTION_BASE_URL"),
        "model": _env("MATRIXARK_EXTRACTION_MODEL"),
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
    skills: Json = {
        "shared_skill_budget_ratio": _env("MATRIXARK_SHARED_SKILL_BUDGET_RATIO", "0.10"),
        "max_budget_tokens": _env("MATRIXARK_MAX_BUDGET_TOKENS", "8192"),
        "skill_chunks_per_skill": _env("MATRIXARK_SKILL_CHUNKS_PER_SKILL", "3"),
        "skill_reserved_refs": _env("MATRIXARK_SKILL_RESERVED_REFS", "3"),
    }

    warnings: List[str] = []
    deterministic = {"", "deterministic", "rules", "local"}
    if extraction_provider in deterministic:
        warnings.append(
            "extraction_provider is deterministic: no LLM is called, so ingest stores only what the "
            "local rules extract. Set MATRIXARK_EXTRACTION_PROVIDER=openai_compatible with "
            "MATRIXARK_EXTRACTION_BASE_URL/_MODEL/_API_KEY_ENV to enable model extraction."
        )
    elif not extraction["api_key_configured"]:
        warnings.append(
            "extraction_provider is " + repr(extraction_provider) + " but " + extraction_key_env
            + " is empty: extraction calls will fail and fall back to the deterministic path."
        )
    if embedding_provider in deterministic:
        warnings.append(
            "embedding_provider is deterministic: retrieval uses hash vectors, not semantic "
            "embeddings. Set MATRIXARK_EMBEDDING_PROVIDER and MATRIXARK_REQUIRE_MODEL_EMBEDDINGS=1."
        )
    else:
        if not require_model_embeddings:
            warnings.append(
                "MATRIXARK_REQUIRE_MODEL_EMBEDDINGS is not set: if the encoder becomes unreachable "
                "the gateway falls back to hash vectors instead of failing the request."
            )
        # Two configuration mistakes silently degrade an OpenAI-compatible encoder to hash vectors,
        # and neither is visible from the outside: the request 200s and retrieval still returns
        # plausible results. Both are cheap to check here.
        api_base = embedding["api_base"]
        if api_base and not api_base.rstrip("/").endswith("/v1"):
            warnings.append(
                "MATRIXARK_EMBEDDING_API_BASE (" + api_base + ") does not end in /v1: the endpoint "
                "is built as <base>/embeddings, so an OpenAI-compatible encoder serving "
                "/v1/embeddings is never reached and every vector is a hash fallback."
            )
        if not embedding["api_key_configured"]:
            warnings.append(
                "embedding key env " + embedding_key_env + " is empty: the embedding call is skipped "
                "before it is attempted, even for a local encoder that needs no auth. Set it to any "
                "non-empty placeholder for a local endpoint."
            )

    return {
        "status": "ok",
        "extraction": extraction,
        "embedding": embedding,
        "skills": skills,
        "warnings": warnings,
    }


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
        status, raw = await _stream_spool_to_datanode(cfg, spool_path, size, dkey, content_type)
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
    x_scope = hmap.get("x-scope")
    if x_scope:
        ingest_body["scope"] = x_scope
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


def _probe_datanode(cfg: GatewayConfig) -> Optional[bool]:
    """Best-effort readiness probe against the datanode. None => could not determine (never fatal)."""
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
        return status < 500
    except Exception:
        return None


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
                           {"error": "backend_error", "detail": str(exc)}, rl_headers)

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

    async def app(scope: Json, receive: Callable, send: Callable) -> None:
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
            probe = await asyncio.to_thread(_probe_datanode, cfg)
            datanode = "unknown" if probe is None else ("ok" if probe else "unreachable")
            return await _json(send, 200, {"ready": True, "datanode": datanode})

        # ---- key-management portal UI (static HTML, no auth to FETCH) ------------------------
        # Returns the self-contained portal page. Fetching the static page needs no auth; every
        # ACTION on it calls an admin-gated JSON endpoint, so the page is inert without a valid
        # admin key. Kept before the data routes so it never touches auth/metering/quota.
        if method == "GET" and path == "/v1/admin/portal":
            return await _html(send, 200, _portal_html_bytes())

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
            usage = meter.snapshot()
            return await _json(send, 200, {"status": "ok", "usage": usage, "count": len(usage)})

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
            return await _json(send, 200, _model_config_snapshot())

        # ---- Prometheus scrape (no auth: counters only, no tenant data) ----------------------
        # The gateway had no /metrics, so the customer-facing API surface was invisible to the
        # dashboards while the engine below it was fully instrumented. These are aggregate
        # ingestion counters -- no keys, no tenant identifiers, nothing per-user -- so the endpoint
        # is safe to scrape without credentials, the way an exporter normally is.
        if method == "GET" and path == "/v1/metrics":
            try:
                import matrixark_ingestion_jobs as _jobs
                body = _jobs.prometheus_text()
            except Exception:
                body = "# ingestion job registry unavailable" + chr(10)
            return await _text(send, 200, body, content_type="text/plain; version=0.0.4")

        # ---- ingestion portal page (static HTML, no auth to FETCH) ---------------------------
        # Same posture as the key portal: fetching the page needs nothing, every action on it calls
        # an admin-gated endpoint, so the page is inert without a valid admin key.
        if method == "GET" and path == "/v1/admin/ingestion":
            return await _html(send, 200, _ingestion_portal_html_bytes())

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
            denied = _usage_read_denied(key_record)
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
                                   {"error": "backend_error", "detail": str(exc)})
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
                                   {"error": "backend_error", "detail": str(exc)})
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
                                   {"error": "backend_error", "detail": str(exc)})
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
                                   {"error": "backend_error", "detail": str(exc)}, rl_headers)
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
                {"error": "storage_quota_exceeded", "detail": str(exc)} if status == 507
                else {"error": "backend_error", "detail": str(exc)})
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
                    out["extraction_error"] = str(exc)
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
