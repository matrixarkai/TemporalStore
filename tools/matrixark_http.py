#!/usr/bin/env python3
"""HTTP/JSON facade for the MatrixArk management portal."""

from __future__ import annotations

import subprocess
import sys
import time
import urllib.request

try:
    from tools.matrixark_mcp_core import *
    from tools.matrixark_mcp_core import _mcp_debug_log, adapter_ensure_backend_ready
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import *
    from matrixark_mcp_core import _mcp_debug_log, adapter_ensure_backend_ready

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


def _http_cloud_mode(server: Any) -> bool:
    mode = os.environ.get("MATRIXARK_HTTP_MODE", "").strip().lower()
    if mode in {"cloud", "prod", "production"}:
        return True
    return getattr(getattr(server, "access", None), "mode", "dev") == "enforced"


def _http_trusted_gateway(headers: Any) -> bool:
    if not headers:
        return False
    trusted = headers.get("X-MatrixArk-Trusted-Gateway", "") or headers.get("X-MatrixArk-Gateway-Verified", "")
    return str(trusted).strip().lower() in {"1", "true", "yes", "trusted"}


def _http_has_auth(headers: Any, args: Json) -> bool:
    _http_api_key(headers, args)
    return bool(args.get("api_key")) or _http_trusted_gateway(headers)


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

_CODEX_HOOK_SHARD_SIZE = 10000
_CODEX_HOOK_SYNTHETIC_MARKERS = {
    "probe",
    "smoke",
    "synthetic",
    "manual ingestion",
    "verification",
    "stdin check",
    "cmd stdin check",
    "service publisher",
    "registered codex hook config verification",
    "real marker check",
    "fixed raw ingestion probe",
    "codex-live-probe",
    "codex-cpp-live-probe",
    "debug",
    "test message",
    "proof",
    "reply ok only",
    "current thread fix",
}


def _hook_decode_value(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, bytes):
        return value.decode("utf-8", "replace")
    if isinstance(value, list) and all(isinstance(item, int) for item in value):
        return bytes(value).decode("utf-8", "replace")
    return str(value)


class _HookStoreReader:
    name = "unknown"

    def get_string(self, key: str) -> str | None:
        raise NotImplementedError

    def hget(self, key: str, field: str) -> str | None:
        raise NotImplementedError


class _CppHookStoreReader(_HookStoreReader):
    name = "c++"

    def __init__(self, args: Json) -> None:
        repo_root = Path(__file__).resolve().parents[1]
        sdk_path = repo_root / "sdk/python"
        if str(sdk_path) not in sys.path:
            sys.path.insert(0, str(sdk_path))
        from temporalstore.client import Client, Options  # type: ignore

        lib_path = str(
            args.get("cpp_library_path")
            or os.environ.get("TEMPORALSTORE_LIB")
            or repo_root / "output-ubuntu22/release/sdk/lib/libbcache2.so"
        )
        metaserver = str(args.get("cpp_metaserver") or os.environ.get("MATRIXARK_TEMPORALSTORE_METASERVER") or "127.0.0.1:18000")
        namespace = str(args.get("namespace") or os.environ.get("MATRIXARK_NAMESPACE") or "deploy_ns")
        table = str(args.get("table") or os.environ.get("MATRIXARK_TABLE") or "deploy_table")
        self.client = Client(
            Options(
                metaserver_addr=metaserver,
                namespace_name=namespace,
                table_name=table,
                request_timeout_ms=int(args.get("request_timeout_ms") or 3000),
                io_timeout_ms=int(args.get("io_timeout_ms") or 3000),
            ),
            library_path=lib_path,
        )

    def get_string(self, key: str) -> str | None:
        try:
            return _hook_decode_value(self.client.get_string(key))
        except Exception:
            return None

    def hget(self, key: str, field: str) -> str | None:
        try:
            return _hook_decode_value(self.client.hget(key, field))
        except Exception:
            return None


class _RustServiceHookStoreReader(_HookStoreReader):
    name = "rust-service"

    def __init__(self, args: Json) -> None:
        self.addr = str(args.get("rust_service_addr") or os.environ.get("MATRIXARK_RUST_SERVICE_PROXY_ADDR") or "127.0.0.1:17100")
        self.namespace = str(args.get("namespace") or os.environ.get("MATRIXARK_NAMESPACE") or "deploy_ns")
        self.table = str(args.get("table") or os.environ.get("MATRIXARK_TABLE") or "deploy_table")

    def _post(self, path: str, payload: Json) -> Json | None:
        try:
            request = urllib.request.Request(
                f"http://{self.addr}{path}",
                data=json.dumps(payload).encode("utf-8"),
                headers={"Content-Type": "application/json"},
            )
            with urllib.request.urlopen(request, timeout=2.0) as response:
                parsed = json.loads(response.read().decode("utf-8"))
                return parsed if isinstance(parsed, dict) else None
        except Exception:
            return None

    def get_string(self, key: str) -> str | None:
        payload = {"namespace": self.namespace, "table_name": self.table, "key": key}
        response = self._post("/ProxyService/Get", payload)
        return _hook_decode_value((response or {}).get("response", {}).get("value"))

    def hget(self, key: str, field: str) -> str | None:
        payload = {"namespace": self.namespace, "table_name": self.table, "key": key, "field": field}
        response = self._post("/ProxyService/HGet", payload)
        return _hook_decode_value((response or {}).get("response", {}).get("value"))


class _RustLocalHookStoreReader(_HookStoreReader):
    name = "rust-local-proxy"

    def __init__(self, args: Json) -> None:
        repo_root = Path(__file__).resolve().parents[1]
        self.proxy_bin = str(args.get("rust_proxy_bin") or os.environ.get("MATRIXARK_RUST_PROXY_BIN") or repo_root / "target/release/matrixark_rust_proxy")
        self.namespace = str(args.get("namespace") or os.environ.get("MATRIXARK_NAMESPACE") or "deploy_ns")
        self.table = str(args.get("table") or os.environ.get("MATRIXARK_TABLE") or "deploy_table")
        self.cwd = str(repo_root)

    def _call(self, op: str, **kwargs: Any) -> Json | None:
        env = os.environ.copy()
        env["MATRIXARK_RUST_PROXY_SINGLE_SHOT_DEBUG"] = "1"
        env.setdefault("MATRIXARK_LOCAL_MODE", "no-metaserver")
        env.setdefault("MATRIXARK_TEMPORALSTORE_METASERVER", "local")
        request = {"op": op, "metaserver": "local", "namespace": self.namespace, "table": self.table, **kwargs}
        try:
            completed = subprocess.run(
                [self.proxy_bin, "--debug-single-shot"],
                input=json.dumps(request) + "\n",
                text=True,
                capture_output=True,
                timeout=4,
                env=env,
                cwd=self.cwd,
            )
        except Exception:
            return None
        for line in reversed(completed.stdout.splitlines()):
            stripped = line.strip()
            if stripped.startswith("{"):
                try:
                    parsed = json.loads(stripped)
                    return parsed if isinstance(parsed, dict) else None
                except Exception:
                    continue
        return None

    def get_string(self, key: str) -> str | None:
        return _hook_decode_value((self._call("get_string", key=key) or {}).get("value"))

    def hget(self, key: str, field: str) -> str | None:
        return _hook_decode_value((self._call("hget", key=key, field=field) or {}).get("value"))


def _hook_prefixes(args: Json, backend: str) -> list[str]:
    explicit = str(args.get("prefix") or "").strip()
    if explicit:
        return [explicit]
    if backend == "c++":
        configured = os.environ.get("MATRIXARK_CPP_TEMPORALSTORE_PREFIX")
        return [prefix for prefix in [configured, "matrixark:codex-hook:cpp-live-v2", "matrixark:codex-hook"] if prefix]
    configured = os.environ.get("MATRIXARK_RUST_TEMPORALSTORE_PREFIX")
    return [prefix for prefix in [configured, "matrixark:codex-hook:rust-live-v2", "matrixark:codex-hook:rust"] if prefix]


def _hook_record_count(reader: _HookStoreReader, prefix: str) -> tuple[str | None, int]:
    for key in (f"{prefix}:raw_ingestion:record_count", f"{prefix}:record_count"):
        value = reader.get_string(key)
        try:
            return key, int(str(value))
        except Exception:
            continue
    return None, 0


def _hook_parse_record(raw: str | None) -> Json | None:
    if not raw:
        return None
    try:
        parsed = json.loads(raw)
    except Exception:
        return {"raw": raw}
    return parsed if isinstance(parsed, dict) else {"raw": parsed}


def _hook_message_candidates(record: Json) -> list[tuple[str, str]]:
    role = record.get("role") or record.get("author_role")
    text_value = record.get("text") or record.get("content") or record.get("message") or record.get("query") or record.get("text_preview")
    messages = record.get("messages")
    if isinstance(messages, list):
        rows: list[tuple[str, str]] = []
        for message in messages:
            if not isinstance(message, dict):
                continue
            message_role = message.get("role") or role
            message_text = message.get("content") or message.get("text") or text_value
            rows.append((str(message_role or ""), str(message_text or "")))
        if rows:
            return rows
    envelope = record.get("envelope") if isinstance(record.get("envelope"), dict) else {}
    if not text_value:
        text_value = envelope.get("query") or envelope.get("visible_local_context") or envelope.get("text")
    if not role:
        role = envelope.get("role") or record.get("speaker")
    return [(str(role or ""), str(text_value or ""))]


def _hook_timestamp_ms(record: Json) -> int:
    for key in ("ingestion_time_ms", "hook_observed_at_ms", "timestamp_ms", "created_at_ms", "event_time_ms", "ts_ms"):
        value = record.get(key)
        try:
            if value is not None:
                return int(value)
        except Exception:
            continue
    return 0


def _hook_session_id(record: Json) -> str:
    scope = record.get("scope") if isinstance(record.get("scope"), dict) else {}
    envelope = record.get("envelope") if isinstance(record.get("envelope"), dict) else {}
    for key in ("session_id", "thread_id", "conversation_id", "codex_thread_id"):
        for source in (record, scope, envelope):
            value = source.get(key) if isinstance(source, dict) else None
            if value:
                return str(value)
    return ""


def _hook_is_real_user(record: Json, role: str, text_value: str, *, real_user_only: bool, include_synthetic: bool) -> bool:
    inferred_role = _hook_infer_role(record, role, text_value)
    if real_user_only and inferred_role.lower() != "user":
        return False
    stripped_text = text_value.strip()
    if not stripped_text:
        return False
    lowered_text = stripped_text.lower()
    if lowered_text.startswith(("<environment_context>", "<codex_internal_context>", "<developer_context>", "<system_context>")):
        return False
    if include_synthetic:
        return True
    if record.get("synthetic") is True:
        return False
    marker_blob = " ".join(
        str(record.get(key, ""))
        for key in ("record_class", "retention_class", "origin", "session_id", "source_kind")
    ).lower()
    marker_blob = f"{text_value.lower()} {marker_blob}"
    return not any(marker in marker_blob for marker in _CODEX_HOOK_SYNTHETIC_MARKERS)


def _hook_infer_role(record: Json, role: str, text_value: str) -> str:
    if role:
        return role
    lowered = text_value.strip().lower()
    if lowered.startswith("assistant:") or lowered in {"ok", "done"}:
        return "assistant"
    event_name = str(record.get("codex_api_event") or record.get("event") or "")
    hook_type = str(record.get("hook_type") or "")
    record_type = str(record.get("record_type") or "")
    if event_name == "UserPromptSubmit" or hook_type == "before_llm" or record_type == "agent_message":
        return "user"
    return role


def _hook_row_identity(row: Json) -> tuple[str, str, str]:
    text_key = " ".join(str(row.get("text", "")).lower().split())[:512]
    return (str(row.get("backend") or ""), str(row.get("session_id") or ""), text_key)


def _hook_prefer_row(candidate: Json, current: Json) -> Json:
    candidate_ts = int(candidate.get("timestamp_ms") or 0)
    current_ts = int(current.get("timestamp_ms") or 0)
    if candidate_ts != current_ts:
        return candidate if candidate_ts > current_ts else current
    candidate_projection = str(candidate.get("projection") or "")
    current_projection = str(current.get("projection") or "")
    if candidate_projection == "raw_ingestion" and current_projection != "raw_ingestion":
        return candidate
    if current_projection == "raw_ingestion" and candidate_projection != "raw_ingestion":
        return current
    return candidate if int(candidate.get("sequence") or 0) > int(current.get("sequence") or 0) else current


def _hook_dedupe_rows(rows: list[Json]) -> list[Json]:
    by_identity: dict[tuple[str, str, str], Json] = {}
    for row in rows:
        identity = _hook_row_identity(row)
        existing = by_identity.get(identity)
        by_identity[identity] = row if existing is None else _hook_prefer_row(row, existing)
    return sorted(
        by_identity.values(),
        key=lambda row: (int(row.get("timestamp_ms") or 0), int(row.get("sequence") or 0)),
        reverse=True,
    )


def _hook_collect(reader: _HookStoreReader, prefix: str, args: Json) -> Json:
    top_k = max(1, min(int(args.get("top_k") or 3), 200))
    scan_limit = max(top_k, min(int(args.get("scan_limit") or 500), 5000))
    real_user_only = bool(args.get("real_user_only", True))
    include_synthetic = bool(args.get("include_synthetic", False))
    count_key, record_count = _hook_record_count(reader, prefix)
    rows: list[Json] = []
    if record_count <= 0:
        return {"backend": reader.name, "prefix": prefix, "count_key": count_key, "record_count": record_count, "rows": rows, "status": "empty"}
    first = max(1, record_count - scan_limit + 1)
    for sequence in range(record_count, first - 1, -1):
        candidate_keys = []
        if count_key and ":raw_ingestion:" in count_key:
            candidate_keys.append((f"{prefix}:raw_ingestion:records:{sequence // _CODEX_HOOK_SHARD_SIZE:06d}", f"{sequence:020d}", "raw_ingestion"))
        candidate_keys.extend(
            [
                (f"{prefix}:records:{sequence // _CODEX_HOOK_SHARD_SIZE:06d}", f"{sequence:020d}", "records"),
                (f"{prefix}:records", f"{sequence:020d}", "records-unsharded"),
            ]
        )
        raw = None
        source_key = ""
        source_field = ""
        projection = ""
        for key, field, kind in candidate_keys:
            raw = reader.hget(key, field)
            if raw:
                source_key, source_field, projection = key, field, kind
                break
        record = _hook_parse_record(raw)
        if not record:
            continue
        for role, text_value in _hook_message_candidates(record):
            if not _hook_is_real_user(record, role, text_value, real_user_only=real_user_only, include_synthetic=include_synthetic):
                continue
            inferred_role = _hook_infer_role(record, role, text_value)
            rows.append(
                {
                    "backend": reader.name,
                    "prefix": prefix,
                    "sequence": sequence,
                    "timestamp_ms": _hook_timestamp_ms(record),
                    "session_id": _hook_session_id(record),
                    "role": inferred_role,
                    "text": text_value,
                    "projection": projection,
                    "source_key": source_key,
                    "source_field": source_field,
                    "record_type": record.get("record_type"),
                    "source_kind": record.get("source_kind"),
                    "synthetic": bool(record.get("synthetic", False)),
                    "record_keys": sorted(k for k in record.keys() if k not in {"messages", "text", "content", "message"})[:48],
                }
            )
    rows = _hook_dedupe_rows(rows)
    status = "ok" if len(rows) >= top_k else ("partial" if rows else "no_matching_rows")
    return {"backend": reader.name, "prefix": prefix, "count_key": count_key, "record_count": record_count, "rows": rows[:top_k], "status": status}


def query_codex_hook_messages(args: Json) -> Json:
    backend = str(args.get("backend") or "both").strip().lower()
    readers: list[tuple[str, _HookStoreReader]] = []
    errors: list[Json] = []
    if backend in {"both", "c++", "cpp"}:
        try:
            readers.append(("c++", _CppHookStoreReader(args)))
        except Exception as exc:
            errors.append({"backend": "c++", "error": str(exc)})
    if backend in {"both", "rust", "rust-service"}:
        try:
            readers.append(("rust", _RustServiceHookStoreReader(args)))
        except Exception as exc:
            errors.append({"backend": "rust-service", "error": str(exc)})
    if backend in {"both", "rust", "rust-local"}:
        try:
            readers.append(("rust", _RustLocalHookStoreReader(args)))
        except Exception as exc:
            errors.append({"backend": "rust-local-proxy", "error": str(exc)})
    results: list[Json] = []
    for backend_kind, reader in readers:
        for prefix in _hook_prefixes(args, "c++" if backend_kind == "c++" else "rust"):
            result = _hook_collect(reader, prefix, args)
            results.append(result)
            if result.get("rows"):
                break
    normalized: dict[str, set[str]] = {}
    for result in results:
        normalized[result["backend"]] = {" ".join(str(row.get("text", "")).lower().split())[:240] for row in result.get("rows", [])}
    comparison: list[Json] = []
    for result in results:
        for row in result.get("rows", []):
            text_key = " ".join(str(row.get("text", "")).lower().split())[:240]
            comparison.append(
                {
                    "backend": row.get("backend"),
                    "sequence": row.get("sequence"),
                    "session_id": row.get("session_id"),
                    "text_preview": str(row.get("text", ""))[:220],
                    "seen_in_other_backend": any(text_key in values for name, values in normalized.items() if name != row.get("backend")),
                }
            )
    return {
        "status": "ok",
        "queried_at_ms": int(time.time() * 1000),
        "top_k": max(1, min(int(args.get("top_k") or 3), 200)),
        "real_user_only": bool(args.get("real_user_only", True)),
        "include_synthetic": bool(args.get("include_synthetic", False)),
        "results": results,
        "comparison": comparison,
        "errors": errors,
    }


def make_matrixark_http_handler(server: "MatrixArkMcpServer", static_root: Path) -> type[SimpleHTTPRequestHandler]:
    static_root = static_root.resolve()

    class MatrixArkHttpHandler(SimpleHTTPRequestHandler):
        server_version = "MatrixArkPortal/0.1"
        cloud_mode = _http_cloud_mode(server)

        def __init__(self, *args: Any, **kwargs: Any) -> None:
            super().__init__(*args, directory=str(static_root), **kwargs)

        def log_message(self, format: str, *args: Any) -> None:
            _mcp_debug_log("http " + format % args)

        def end_headers(self) -> None:
            if self.cloud_mode:
                self.send_header("Access-Control-Allow-Origin", os.environ.get("MATRIXARK_HTTP_ALLOWED_ORIGIN", "https://app.matrixark.ai"))
            else:
                self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header(
                "Access-Control-Allow-Headers",
                "Authorization, Content-Type, X-MatrixArk-API-Key, X-MatrixArk-Trusted-Gateway, X-MatrixArk-Gateway-Verified",
            )
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

        def _write_auth_required(self, tool_name: str = "http") -> None:
            self._write_json(
                401,
                {
                    "status": "error",
                    "tool": tool_name,
                    "error": "MatrixArk cloud HTTP API requires bearer API key or trusted gateway authentication",
                },
            )

        def _call_tool_route(self, tool_name: str, args: Json) -> None:
            try:
                if self.cloud_mode and not _http_has_auth(self.headers, args):
                    self._write_auth_required(tool_name)
                    return
                _http_api_key(self.headers, args)
                result = server.call_tool(tool_name, args)
                self._write_json(200, {"status": "ok", "tool": tool_name, "result": result})
            except Exception as exc:
                self._write_json(500, {"status": "error", "tool": tool_name, "error": str(exc)})

        def do_GET(self) -> None:  # noqa: N802 - http.server API
            parsed = urlparse(self.path)
            if parsed.path == "/api/health":
                backend = "unknown"
                try:
                    backend = str(server.adapter.backend_metrics().get("backend", "unknown"))
                except Exception:
                    pass
                self._write_json(200, {"status": "ok", "service": "matrixark_portal", "backend": backend})
                return
            if parsed.path == "/api/ready":
                try:
                    ready = adapter_ensure_backend_ready(server.adapter, reason="http_ready", probe=True, timeout_ms=1000)
                    status = "ok" if ready.get("status") == "ready" else "topology_not_ready"
                    self._write_json(200 if status == "ok" else 503, {"status": status, "service": "matrixark_portal", "readiness": ready})
                except Exception as exc:
                    self._write_json(503, {"status": "topology_not_ready", "service": "matrixark_portal", "error": str(exc)})
                return
            if parsed.path in HTTP_TOOL_ROUTES:
                self._call_tool_route(HTTP_TOOL_ROUTES[parsed.path], _http_query_args(parsed))
                return
            if parsed.path == "/api/codex_hook_messages":
                args = _http_query_args(parsed)
                if self.cloud_mode and not _http_has_auth(self.headers, args):
                    self._write_auth_required("codex_hook_messages")
                    return
                self._write_json(200, {"status": "ok", "result": query_codex_hook_messages(args)})
                return
            if parsed.path == "/api/tools":
                args: Json = {}
                if self.cloud_mode and not _http_has_auth(self.headers, args):
                    self._write_auth_required("tools")
                    return
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
            if parsed.path == "/api/codex_hook_messages":
                args = body.get("arguments") if isinstance(body.get("arguments"), dict) else body
                if self.cloud_mode and not _http_has_auth(self.headers, args):
                    self._write_auth_required("codex_hook_messages")
                    return
                self._write_json(200, {"status": "ok", "result": query_codex_hook_messages(args)})
                return
            self._write_json(404, {"status": "error", "error": f"unknown API path {parsed.path}"})

    return MatrixArkHttpHandler
