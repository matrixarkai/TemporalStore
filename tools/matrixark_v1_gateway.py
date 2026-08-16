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

Routes: POST /v1/ingest (async fast-ack 202) · POST /v1/session/commit · POST /v1/retrieve ·
        POST /v1/mcp · PUT|POST|GET /v1/blob/<key> · GET /v1/healthz|/readyz. Everything that is not
        under `/v1` is delegated to the legacy `make_asgi_app(server)` so `/api/*`, `/mcp`, `/healthz`
        keep working unchanged (back-compat).
"""
from __future__ import annotations

import asyncio
import json
import math
import os
import threading
import time
from typing import Any, Awaitable, Callable, Optional, Tuple
from urllib.parse import urlparse

try:
    from tools.matrixark_asgi import make_asgi_app, _api_key
    from tools.matrixark_http import apply_ingest_route_defaults, mcp_http_dispatch
except ImportError:  # Direct script execution from tools/.
    from matrixark_asgi import make_asgi_app, _api_key  # type: ignore
    from matrixark_http import apply_ingest_route_defaults, mcp_http_dispatch  # type: ignore

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

# ---- defaults (all overridable via env or the `config` dict) -----------------------------------
_MIB = 1024 * 1024
_GIB = 1024 * 1024 * 1024
_DEFAULTS = {
    "require_auth": True,
    "enforced": False,
    "ingest_rps": 5000.0,
    "ingest_burst": 10000.0,
    "retrieve_rps": 6000.0,
    "retrieve_burst": 12000.0,
    "blob_streams": 500,
    "max_body_bytes": 16 * _MIB,
    "max_batch": 1000,
    "max_blob_bytes": 5 * _GIB,
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
}

# route -> (tool name, rate-limit class). "__mcp__" is dispatched through mcp_http_dispatch.
_DATA_ROUTES: dict[str, Tuple[str, str]] = {
    "/v1/ingest": ("matrixark_ingest", "ingest"),
    "/v1/session/commit": ("matrixark_session_commit", "ingest"),
    "/v1/retrieve": ("matrixark_retrieve", "retrieve"),
    "/v1/mcp": ("__mcp__", "retrieve"),
}

# Backend exception class names we translate to specific edge status codes (matched by name to avoid
# a hard import dependency on the backend package).
_BACKPRESSURE_ERRORS = {"MatrixArkBackpressureError"}
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


def _parse_hashed_keys(env: Any, overrides: Json) -> dict[str, Json]:
    """Enforced-mode keystore: ``{api_key_hash -> {"tenant_id", "account_id"}}``.

    Keys are stored HASHED, never plaintext. The store is provisioned by
    ``matrixark_provision_api_key.py`` (which reuses the backend ``secret_hash`` and the
    ``matrixark_api_key`` record shape). Two source shapes are accepted from
    ``MATRIXARK_API_KEYS_HASHED_FILE``:

      * JSONL of ``matrixark_api_key`` records (same fields the backend credential store writes:
        ``record_type``/``api_key_hash``/``account_id``/``tenant_id``/``status``/``expires_at_ms``).
      * a plain JSON object ``{"<sha256hex>": {"tenant_id": ..., "account_id": ...}}``.

    ``overrides["hashed_api_keys"]`` (a dict) short-circuits for tests. Inactive/expired records are
    skipped. The last active record for a given hash wins.
    """
    if isinstance(overrides.get("hashed_api_keys"), dict):
        return {str(k): dict(v) if isinstance(v, dict) else {"tenant_id": str(v)}
                for k, v in overrides["hashed_api_keys"].items()}
    path = str(env.get("MATRIXARK_API_KEYS_HASHED_FILE", "") or "").strip()
    out: dict[str, Json] = {}
    if not path or not os.path.exists(path):
        return out
    now = int(time.time() * 1000)

    def _add(hash_hex: str, tenant_id: str, account_id: str) -> None:
        if hash_hex and tenant_id:
            out[hash_hex] = {"tenant_id": tenant_id, "account_id": account_id or "acct_local"}

    with open(path, "r", encoding="utf-8") as handle:
        text = handle.read()
    stripped = text.strip()
    if stripped.startswith("{") and '"api_key_hash"' not in stripped:
        # plain JSON object form.
        try:
            data = json.loads(stripped)
        except json.JSONDecodeError:
            data = {}
        if isinstance(data, dict):
            for hash_hex, value in data.items():
                if isinstance(value, dict):
                    _add(str(hash_hex), str(value.get("tenant_id") or ""), str(value.get("account_id") or ""))
                else:
                    _add(str(hash_hex), str(value), "")
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
        _add(str(record.get("api_key_hash") or ""), str(record.get("tenant_id") or ""),
             str(record.get("account_id") or ""))
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
# Auth + tenant isolation
# ================================================================================================
def _authorize(headers: list[Tuple[bytes, bytes]],
               cfg: GatewayConfig) -> Tuple[bool, Optional[str], Optional[str], Optional[str]]:
    """Resolve the Bearer key to a tenant identity.

    Returns ``(allowed, api_key, tenant_id, account_id)``.

    ENFORCED mode (``MATRIXARK_AUTH_ENFORCED=1``): EVERY /v1 request must carry a Bearer key whose
    sha256 hash is present in the provisioned hashed keystore. A missing, unknown, or revoked key --
    and the legacy demo key ``sk_live_demo`` (which is never hashed into the enforced store) -- is
    rejected. The identity ``{tenant_id, account_id}`` is taken from the stored record, NEVER from
    client-supplied free text, so two tenants cannot collide on a shared scope string.

    DEV/unenforced mode (default): unchanged legacy behavior -- the plaintext ``api_keys`` env map is
    honored and, when ``require_auth`` is off, anonymous requests are allowed. This keeps existing dev
    flows (the demo key) working when enforcement is off.
    """
    key = _api_key(headers)
    if cfg.enforced:
        if not key:
            return False, None, None, None
        record = cfg.hashed_keys.get(_secret_hash(key))
        if record:
            return True, key, str(record.get("tenant_id") or ""), str(record.get("account_id") or "") or None
        return False, None, None, None
    # ---- dev / unenforced (legacy) --------------------------------------------------------------
    if key and key in cfg.api_keys:
        return True, key, cfg.api_keys[key], None
    if not cfg.require_auth:  # local/dev: anonymous is allowed.
        return True, key, (cfg.api_keys.get(key) if key else None) or "anonymous", None
    return False, None, None, None


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
    legacy = make_asgi_app(server)
    limiter = _RateLimiter(cfg)
    executor_state = {"installed": False}
    executor_lock = threading.Lock()
    # Direct network path (B): one shared pooled client per worker when enabled.
    direct_client = _build_direct_client(cfg) if cfg.direct_backend else None

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

        # ---- blob (auth + concurrent-stream cap, streamed) ----------------------------------
        if path.startswith("/v1/blob/"):
            allowed, key, tenant, account = _authorize(scope.get("headers", []), cfg)
            if not allowed:
                return await _json(send, 401, {"error": "unauthorized"})
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

        # ---- data routes --------------------------------------------------------------------
        route = _DATA_ROUTES.get(path)
        if route is None:
            return await _json(send, 404, {"error": "not_found"})
        tool, cls = route
        if method != "POST":
            return await _json(send, 405, {"error": "method_not_allowed"})

        allowed, key, tenant, account = _authorize(scope.get("headers", []), cfg)
        if not allowed:
            return await _json(send, 401, {"error": "unauthorized"})

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

        # MCP-over-HTTP: dispatch the JSON-RPC message directly (api-key injected downstream).
        if tool == "__mcp__":
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
    return MatrixArkMcpServer(adapter, access_mode=os.environ.get("MATRIXARK_ACCESS_MODE", "enforced"))


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
