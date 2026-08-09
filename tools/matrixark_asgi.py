#!/usr/bin/env python3
"""Production ASGI front for MatrixArk: scaled HTTP ingestion + MCP-over-HTTP, in pure Python.

Replaces the stdlib ThreadingHTTPServer for enterprise scale. Run under any ASGI server:
    uvicorn matrixark_asgi:application --workers 4        # or gunicorn -k uvicorn.workers.UvicornWorker

Design constraints honored here:
  * Only TemporalStore (the storage engine) is Rust; this front is 100% Python.
  * No queue is held in MatrixArk — durability lives in TemporalStore's async ingestion (it stores the
    raw ContextEvent fast, then extracts async = the queue). Kafka, if used at all, sits *before*
    extraction for extreme scale / fan-out; this FE just hands off and returns.

It is framework-free (raw ASGI) so the module imports with no server lib installed; the ASGI server is
a runtime dependency only when you actually serve. The MatrixArk backend (server.call_tool / mcp
dispatch) is synchronous, so it runs in a threadpool to keep the event loop free for high concurrency.

Routes: POST /api/ingest (async fast-ack) · POST /api/retrieve · POST /api/session_commit ·
        POST /mcp (MCP-over-HTTP, stateless) · GET /healthz|/readyz.
"""
from __future__ import annotations

import asyncio
import json
from typing import Any, Awaitable, Callable, Optional

try:
    from tools.matrixark_http import apply_ingest_route_defaults, mcp_http_dispatch
except ImportError:
    from matrixark_http import apply_ingest_route_defaults, mcp_http_dispatch

Json = dict[str, Any]

_ROUTES = {
    "/api/ingest": "matrixark_ingest",
    "/api/retrieve": "matrixark_retrieve",
    "/api/session_commit": "matrixark_session_commit",
}


def _api_key(headers: list[tuple[bytes, bytes]]) -> Optional[str]:
    hm = {k.lower(): v for k, v in headers}
    auth = hm.get(b"authorization", b"").decode("latin-1")
    if auth.lower().startswith("bearer "):
        return auth[7:].strip() or None
    xk = hm.get(b"x-api-key", b"").decode("latin-1").strip()
    return xk or None


def make_asgi_app(server: Any) -> Callable[..., Awaitable[None]]:
    """Return an ASGI app that fronts an already-built MatrixArk server (the integration point)."""

    async def app(scope: Json, receive: Callable, send: Callable) -> None:
        if scope.get("type") != "http":
            return
        path = scope.get("path", "")
        method = scope.get("method", "")
        if method == "GET" and path in ("/healthz", "/readyz"):
            return await _json(send, 200, {"status": "ok"})
        if method != "POST":
            return await _json(send, 405, {"status": "error", "error": "method not allowed"})
        raw = await _read_body(receive)
        try:
            parsed = json.loads(raw or b"{}")
            if not isinstance(parsed, dict):
                raise ValueError("body must be a JSON object")
        except (json.JSONDecodeError, ValueError) as exc:
            return await _json(send, 400, {"status": "error", "error": f"bad json body: {exc}"})
        api_key = _api_key(scope.get("headers", []))
        try:
            if path == "/mcp":
                resp = await asyncio.to_thread(mcp_http_dispatch, server, parsed, api_key=api_key)
                return await _json(send, 200, resp)
            if path in _ROUTES:
                args = parsed.get("arguments") if isinstance(parsed.get("arguments"), dict) else parsed
                if not isinstance(args, dict):
                    args = {}
                if api_key:
                    args.setdefault("api_key", api_key)
                apply_ingest_route_defaults(path, args)
                result = await asyncio.to_thread(server.call_tool, _ROUTES[path], args)
                return await _json(send, 200, {"status": "ok", "tool": _ROUTES[path], "result": result})
            return await _json(send, 404, {"status": "error", "error": f"unknown path {path}"})
        except Exception as exc:
            # Shed load under overload -> 429 (client SDK backs off); everything else -> 500.
            if exc.__class__.__name__ == "MatrixArkBackpressureError":
                return await _json(send, 429, {"status": "overloaded", "error": str(exc), "retry_after_ms": 100})
            return await _json(send, 500, {"status": "error", "error": str(exc)})

    return app


async def _read_body(receive: Callable) -> bytes:
    chunks: list[bytes] = []
    while True:
        msg = await receive()
        if msg.get("type") != "http.request":
            break
        chunks.append(msg.get("body", b"") or b"")
        if not msg.get("more_body"):
            break
    return b"".join(chunks)


async def _json(send: Callable, status: int, payload: Json) -> None:
    data = json.dumps(payload).encode("utf-8")
    await send({
        "type": "http.response.start",
        "status": status,
        "headers": [(b"content-type", b"application/json"), (b"content-length", str(len(data)).encode())],
    })
    await send({"type": "http.response.body", "body": data})


def create_app() -> Callable[..., Awaitable[None]]:
    """Build the MatrixArk server from env and return the ASGI app (for `uvicorn matrixark_asgi:application`).

    The real integration point is make_asgi_app(server); deployers usually build the adapter/server the
    same way the MCP entrypoint does and call make_asgi_app directly. This convenience uses env defaults.
    """
    import os
    try:
        from tools.matrixark_mcp_server import MatrixArkMcpServer
        from tools.matrixark_mcp_backends import build_mcp_adapter, default_mcp_backend
    except ImportError:
        from matrixark_mcp_server import MatrixArkMcpServer  # type: ignore
        from matrixark_mcp_backends import build_mcp_adapter, default_mcp_backend  # type: ignore
    import argparse
    ns = argparse.Namespace(backend=os.environ.get("MATRIXARK_MCP_BACKEND", default_mcp_backend()))
    adapter = build_mcp_adapter(ns)
    server = MatrixArkMcpServer(adapter, access_mode=os.environ.get("MATRIXARK_ACCESS_MODE", "enforced"))
    return make_asgi_app(server)


def main() -> int:
    import os
    try:
        import uvicorn
    except ImportError:
        raise SystemExit(
            "uvicorn is not installed. The scaled Python front runs under an ASGI server:\n"
            "    pip install uvicorn && uvicorn matrixark_asgi:application --workers 4"
        )
    uvicorn.run(
        create_app(),
        host=os.environ.get("MATRIXARK_HTTP_HOST", "0.0.0.0"),
        port=int(os.environ.get("MATRIXARK_HTTP_PORT", "8080")),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
