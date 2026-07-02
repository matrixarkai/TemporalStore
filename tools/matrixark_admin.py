#!/usr/bin/env python3
"""Small MatrixArk admin CLI over the same MCP server implementation."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

try:
    from tools.matrixark_mcp_server import MatrixArkLocalAdapter, MatrixArkMcpServer, main as mcp_main
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_server import MatrixArkLocalAdapter, MatrixArkMcpServer, main as mcp_main


def http_portal_main() -> int:
    os.environ.setdefault("MATRIXARK_HTTP_PORT", os.environ.get("MATRIXARK_HTTP_PORT", "8080"))
    return mcp_main()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("tool", help="MatrixArk admin/auth/portal tool name, for example matrixark_management_portal.")
    parser.add_argument("--arguments-json", default="{}", help="JSON object passed as tool arguments.")
    parser.add_argument("--event-log", type=Path, default=Path(os.environ.get("MATRIXARK_ADMIN_EVENT_LOG", "/tmp/matrixark-admin-events.jsonl")))
    parser.add_argument("--access-mode", choices=["dev", "enforced"], default=os.environ.get("MATRIXARK_ACCESS_MODE", "dev"))
    args = parser.parse_args()
    payload = json.loads(args.arguments_json)
    if not isinstance(payload, dict):
        raise SystemExit("--arguments-json must decode to an object")
    server = MatrixArkMcpServer(MatrixArkLocalAdapter(args.event_log), line_json=True, access_mode=args.access_mode)
    print(json.dumps(server.call_tool(args.tool, payload), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
