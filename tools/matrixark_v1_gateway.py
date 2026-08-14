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

Json = dict[str, Any]

# ---- defaults (all overridable via env or the `config` dict) -----------------------------------
_MIB = 1024 * 1024
_GIB = 1024 * 1024 * 1024
_DEFAULTS = {
    "require_auth": True,
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


class GatewayConfig:
    """Resolved gateway configuration (env + optional overrides)."""

    def __init__(self, **fields: Any) -> None:
        for name, value in _DEFAULTS.items():
            setattr(self, name, fields.get(name, value))
        self.api_keys: dict[str, str] = fields.get("api_keys", {})
        # Datanode blob target, parsed for the http.client proxy.
        parsed = urlparse(str(self.datanode_url))
        self.blob_scheme = parsed.scheme or "http"
        self.blob_host = parsed.hostname or "127.0.0.1"
        self.blob_port = parsed.port or (443 if self.blob_scheme == "https" else 17102)
        # Injectable so tests can proxy without a network; production builds an http.client conn.
        self.blob_connection_factory: Callable[["GatewayConfig"], Any] = fields.get(
            "blob_connection_factory", _default_blob_connection
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
            require_auth=_env_bool(
                overrides.get("require_auth", env.get("MATRIXARK_REQUIRE_AUTH")),
                _DEFAULTS["require_auth"],
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
            blob_connection_factory=overrides.get("blob_connection_factory", _default_blob_connection),
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
def _authorize(headers: list[Tuple[bytes, bytes]], cfg: GatewayConfig) -> Tuple[bool, Optional[str], Optional[str]]:
    """Return (allowed, api_key, tenant). Unknown/missing key -> (False, ...) unless auth disabled."""
    key = _api_key(headers)
    if key and key in cfg.api_keys:
        return True, key, cfg.api_keys[key]
    if not cfg.require_auth:  # local/dev: anonymous is allowed.
        return True, key, (cfg.api_keys.get(key) if key else None) or "anonymous"
    return False, None, None


def _apply_identity(args: Json, key: Optional[str], tenant: Optional[str]) -> Json:
    """Inject api_key/tenant and namespace-isolate `scope` under the tenant (guarding double-prefix)."""
    if key:
        args["api_key"] = key
    if tenant:
        args["tenant"] = tenant
        scope = args.get("scope")
        if isinstance(scope, str) and scope:
            if not (scope == tenant or scope.startswith(tenant + "/")):
                args["scope"] = f"{tenant}/{scope}"
        else:
            args["scope"] = tenant
    return args


def _isolate_key(key_str: str, tenant: Optional[str]) -> str:
    if not tenant:
        return key_str
    if key_str == tenant or key_str.startswith(tenant + "/"):
        return key_str
    return f"{tenant}/{key_str}"


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


# ================================================================================================
# Blob proxy (streamed, bounded memory)
# ================================================================================================
async def _blob_put(scope: Json, receive: Callable, send: Callable, method: str,
                    cfg: GatewayConfig, key_str: str, tenant: Optional[str]) -> None:
    hmap = _headers_map(scope)
    cl_raw = hmap.get("content-length")
    declared = int(cl_raw) if cl_raw and cl_raw.isdigit() else None
    if declared is not None and declared > cfg.max_blob_bytes:
        return await _json(send, 413, {"error": "payload_too_large"})

    dkey = _isolate_key(key_str, tenant)
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


async def _blob_get(send: Callable, cfg: GatewayConfig, key_str: str, tenant: Optional[str]) -> None:
    dkey = _isolate_key(key_str, tenant)
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
def make_v1_app(server: Any, config: Any = None) -> Callable[..., Awaitable[None]]:
    """Return the `/v1/*` gateway ASGI app fronting an already-built MatrixArk `server`.

    Non-`/v1` paths fall through to the legacy `make_asgi_app(server)` so `/api/*`, `/mcp` and the
    bare `/healthz` keep working unchanged.
    """
    cfg = _coerce_config(config)
    legacy = make_asgi_app(server)
    limiter = _RateLimiter(cfg)

    async def app(scope: Json, receive: Callable, send: Callable) -> None:
        if scope.get("type") != "http":
            return
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
            allowed, key, tenant = _authorize(scope.get("headers", []), cfg)
            if not allowed:
                return await _json(send, 401, {"error": "unauthorized"})
            key_str = path[len("/v1/blob/"):]
            if not key_str:
                return await _json(send, 404, {"error": "not_found"})
            if not limiter.blob_acquire():
                return await _json(send, 429, {"error": "rate_limited"}, [(b"retry-after", b"1")])
            try:
                if method in ("PUT", "POST"):
                    return await _blob_put(scope, receive, send, method, cfg, key_str, tenant)
                if method == "GET":
                    return await _blob_get(send, cfg, key_str, tenant)
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

        allowed, key, tenant = _authorize(scope.get("headers", []), cfg)
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
                resp = await asyncio.to_thread(mcp_http_dispatch, server, parsed, api_key=key)
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

        apply_ingest_route_defaults("/api/ingest" if tool == "matrixark_ingest" else path, args)
        _apply_identity(args, key, tenant)

        try:
            result = await asyncio.to_thread(server.call_tool, tool, args)
        except Exception as exc:
            status = _classify_backend_error(exc)
            body = {"error": "rate_limited"} if status == 429 else (
                {"error": "storage_quota_exceeded", "detail": str(exc)} if status == 507
                else {"error": "backend_error", "detail": str(exc)})
            headers = rl_headers + ([(b"retry-after", b"1")] if status == 429 else [])
            return await _json(send, status, body, headers)

        if tool == "matrixark_ingest":
            accepted = n_records if n_records is not None else (n_messages or 0)
            return await _json(send, 202,
                               {"accepted": accepted, "scope": args.get("scope"), "result": result},
                               rl_headers)
        return await _json(send, 200, _ok_body(result), rl_headers)

    return app


# ================================================================================================
# Entrypoints
# ================================================================================================
def _build_server_from_env() -> Any:
    import argparse

    try:
        from tools.matrixark_mcp_server import MatrixArkMcpServer
        from tools.matrixark_mcp_backends import build_mcp_adapter, default_mcp_backend
    except ImportError:
        from matrixark_mcp_server import MatrixArkMcpServer  # type: ignore
        from matrixark_mcp_backends import build_mcp_adapter, default_mcp_backend  # type: ignore
    ns = argparse.Namespace(backend=os.environ.get("MATRIXARK_MCP_BACKEND", default_mcp_backend()))
    adapter = build_mcp_adapter(ns)
    return MatrixArkMcpServer(adapter, access_mode=os.environ.get("MATRIXARK_ACCESS_MODE", "enforced"))


def create_v1_app() -> Callable[..., Awaitable[None]]:
    """Build the MatrixArk server from env and return the `/v1/*` gateway ASGI app.

    Mirrors `matrixark_asgi.create_app` for `uvicorn matrixark_v1_gateway:application`.
    """
    return make_v1_app(_build_server_from_env(), GatewayConfig.from_env())


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
