#!/usr/bin/env python3
"""HTTP/JSON facade for the MatrixArk management portal."""

from __future__ import annotations

try:
    from tools.matrixark_mcp_core import *
    from tools.matrixark_mcp_core import _mcp_debug_log
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import *
    from matrixark_mcp_core import _mcp_debug_log

def _coerce_http_value(value: str) -> Any:
    lowered = value.strip().lower()
    if lowered in {"true", "false"}:
        return lowered == "true"
    if value.isdigit() or (value.startswith("-") and value[1:].isdigit()):
        try:
            return int(value)
        except ValueError:
            return value
    return value


def _http_query_args(parsed: Any) -> Json:
    args: Json = {}
    scope: Json = {}
    for key, values in parse_qs(parsed.query, keep_blank_values=True).items():
        if not values:
            continue
        value: Any = _coerce_http_value(values[-1])
        if key.startswith("scope."):
            scope[key.split(".", 1)[1]] = value
        elif key in {"account_id", "tenant_id", "user_id", "session_id", "agent_name", "team", "project"}:
            scope[key] = value
            args[key] = value
        else:
            args[key] = value
    if scope:
        args["scope"] = scope
    return args


def _http_api_key(headers: Any, args: Json) -> None:
    authorization = headers.get("Authorization", "") if headers else ""
    if authorization.lower().startswith("bearer "):
        args.setdefault("api_key", authorization.split(" ", 1)[1].strip())
    header_key = headers.get("X-MatrixArk-API-Key", "") if headers else ""
    if header_key:
        args.setdefault("api_key", header_key.strip())


HTTP_TOOL_ROUTES: dict[str, str] = {
    "/api/management_portal": "matrixark_management_portal",
    "/api/ingestion_dashboard": "matrixark_ingestion_dashboard",
    "/api/backend_metrics": "matrixark_backend_metrics",
    "/api/audit": "matrixark_admin_audit",
    "/api/replay": "matrixark_replay",
    "/api/auth/signup": "matrixark_auth_signup",
    "/api/auth/sso_login": "matrixark_auth_sso_login",
    "/api/auth/sso_callback": "matrixark_auth_sso_callback",
    "/api/admin/create_account": "matrixark_admin_create_account",
    "/api/admin/create_user": "matrixark_admin_create_user",
    "/api/admin/create_api_key": "matrixark_admin_create_api_key",
    "/api/admin/list_api_keys": "matrixark_admin_list_api_keys",
    "/api/admin/rotate_api_key": "matrixark_admin_rotate_api_key",
    "/api/admin/revoke_api_key": "matrixark_admin_revoke_api_key",
    "/api/admin/audit": "matrixark_admin_audit",
}


def make_matrixark_http_handler(server: "MatrixArkMcpServer", static_root: Path) -> type[SimpleHTTPRequestHandler]:
    static_root = static_root.resolve()

    class MatrixArkHttpHandler(SimpleHTTPRequestHandler):
        server_version = "MatrixArkPortal/0.1"

        def __init__(self, *args: Any, **kwargs: Any) -> None:
            super().__init__(*args, directory=str(static_root), **kwargs)

        def log_message(self, format: str, *args: Any) -> None:
            _mcp_debug_log("http " + format % args)

        def end_headers(self) -> None:
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Access-Control-Allow-Headers", "Authorization, Content-Type, X-MatrixArk-API-Key")
            self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
            super().end_headers()

        def do_OPTIONS(self) -> None:  # noqa: N802 - http.server API
            self.send_response(204)
            self.end_headers()

        def _write_json(self, status: int, payload: Json) -> None:
            body = json.dumps(payload, sort_keys=True).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _read_json_body(self) -> Json:
            length = int(self.headers.get("Content-Length", "0") or "0")
            if length <= 0:
                return {}
            raw = self.rfile.read(length)
            if not raw:
                return {}
            parsed = json.loads(raw.decode("utf-8"))
            if not isinstance(parsed, dict):
                raise MatrixArkError("HTTP JSON body must be an object")
            return parsed

        def _call_tool_route(self, tool_name: str, args: Json) -> None:
            try:
                _http_api_key(self.headers, args)
                result = server.call_tool(tool_name, args)
                self._write_json(200, {"status": "ok", "tool": tool_name, "result": result})
            except Exception as exc:
                self._write_json(500, {"status": "error", "tool": tool_name, "error": str(exc)})

        def do_GET(self) -> None:  # noqa: N802 - http.server API
            parsed = urlparse(self.path)
            if parsed.path in {"/api/health", "/api/ready"}:
                self._write_json(200, {"status": "ok", "service": "matrixark_portal", "backend": server.adapter.backend_metrics().get("backend", "unknown")})
                return
            if parsed.path in HTTP_TOOL_ROUTES:
                self._call_tool_route(HTTP_TOOL_ROUTES[parsed.path], _http_query_args(parsed))
                return
            if parsed.path == "/api/tools":
                self._write_json(200, {"status": "ok", "tools": sorted(HTTP_TOOL_ROUTES.values())})
                return
            super().do_GET()

        def do_POST(self) -> None:  # noqa: N802 - http.server API
            parsed = urlparse(self.path)
            try:
                body = self._read_json_body()
            except Exception as exc:
                self._write_json(400, {"status": "error", "error": str(exc)})
                return
            if parsed.path == "/api/tools/call":
                tool_name = str(body.get("tool") or body.get("name") or "")
                args = body.get("arguments") or body.get("args") or {}
                if not tool_name or not isinstance(args, dict):
                    self._write_json(400, {"status": "error", "error": "body requires tool and arguments object"})
                    return
                self._call_tool_route(tool_name, args)
                return
            if parsed.path in HTTP_TOOL_ROUTES:
                args = body.get("arguments") if isinstance(body.get("arguments"), dict) else body
                self._call_tool_route(HTTP_TOOL_ROUTES[parsed.path], args)
                return
            self._write_json(404, {"status": "error", "error": f"unknown API path {parsed.path}"})

    return MatrixArkHttpHandler

