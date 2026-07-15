#!/usr/bin/env python3
"""Command-line bootstrap for MatrixArk MCP server."""

from __future__ import annotations

import argparse
import os
from pathlib import Path

try:
    from tools.matrixark_mcp_backends import add_backend_arguments, build_mcp_adapter, ensure_startup_backend_ready
    from tools.matrixark_mcp_core import _mcp_debug_log
    from tools.matrixark_mcp_server import MatrixArkMcpServer
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_backends import add_backend_arguments, build_mcp_adapter, ensure_startup_backend_ready
    from matrixark_mcp_core import _mcp_debug_log
    from matrixark_mcp_server import MatrixArkMcpServer


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    add_backend_arguments(parser)
    parser.add_argument(
        "--line-json",
        action="store_true",
        help="Use newline-delimited JSON for simple shell debugging instead of MCP framing.",
    )
    parser.add_argument(
        "--http-host",
        default=os.environ.get("MATRIXARK_HTTP_HOST", "127.0.0.1"),
        help="Host for the optional HTTP/JSON management portal facade.",
    )
    parser.add_argument(
        "--http-port",
        type=int,
        default=int(os.environ.get("MATRIXARK_HTTP_PORT", "0")),
        help="If non-zero, serve the browser portal and /api JSON facade instead of stdio MCP.",
    )
    parser.add_argument(
        "--http-root",
        type=Path,
        default=Path(os.environ.get("MATRIXARK_HTTP_ROOT", str(Path(__file__).resolve().parent / "temporalstore-monitoring-ui"))),
        help="Static document root for HTTP portal mode.",
    )
    parser.add_argument(
        "--access-mode",
        choices=["dev", "enforced"],
        default=os.environ.get("MATRIXARK_ACCESS_MODE", "dev"),
        help="dev allows omitted API keys for local testing; enforced requires scoped MatrixArk API keys.",
    )
    args = parser.parse_args()
    if getattr(args, "rust_direct_lib", ""):
        os.environ["MATRIXARK_TEMPORALSTORE_RUST_DIRECT_LIB"] = args.rust_direct_lib
    _mcp_debug_log(f"main: parsed backend={args.backend} metaserver={args.metaserver}")
    adapter = build_mcp_adapter(args)
    ensure_startup_backend_ready(adapter, args.backend)
    _mcp_debug_log("main: adapter ready; serving")
    mcp_server = MatrixArkMcpServer(adapter, line_json=args.line_json, access_mode=args.access_mode)
    if args.http_port:
        mcp_server.serve_http(host=args.http_host, port=args.http_port, static_root=args.http_root)
    else:
        mcp_server.serve()
    _mcp_debug_log("main: serve returned")
    return 0



if __name__ == "__main__":
    raise SystemExit(main())
