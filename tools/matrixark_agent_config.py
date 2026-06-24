#!/usr/bin/env python3
"""Generate MatrixArk agent integration snippets.

The MatrixArk MCP server is the common boundary for Codex, Claude Desktop,
Cursor, and vertical AI agents.  This helper emits copy-pasteable config for
the common clients without requiring each integration doc to drift.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path


DEFAULT_REPO_ROOT = "/root/src/github-services/TemporalStore"
DEFAULT_LAUNCHER = "tools/matrixark_mcp_cpp_server.sh"


def wsl_args(repo_root: str, launcher: str) -> list[str]:
    return [
        "--cd",
        repo_root,
        "-e",
        "bash",
        "-lc",
        f"exec {launcher}",
    ]


def stdio_server(repo_root: str, launcher: str) -> dict[str, object]:
    return {
        "command": "wsl.exe",
        "args": wsl_args(repo_root, launcher),
        "env": {
            "MATRIXARK_LOCAL_MODE": "cluster",
            "MATRIXARK_MCP_BACKEND": "temporalstore-direct",
            "MATRIXARK_RETRIEVAL_TIMEOUT_MS": "5000",
        },
    }


def codex_toml(repo_root: str, launcher: str) -> str:
    args = ", ".join(f"'{item}'" for item in wsl_args(repo_root, launcher))
    return "\n".join(
        [
            "[mcp_servers.matrixark]",
            "command = 'wsl.exe'",
            f"args = [ {args} ]",
            "startup_timeout_sec = 120",
            "",
        ]
    )


def claude_json(repo_root: str, launcher: str) -> str:
    return json.dumps(
        {"mcpServers": {"matrixark": stdio_server(repo_root, launcher)}},
        indent=2,
        sort_keys=True,
    )


def cursor_json(repo_root: str, launcher: str) -> str:
    return json.dumps(
        {"mcpServers": {"matrixark": stdio_server(repo_root, launcher)}},
        indent=2,
        sort_keys=True,
    )


def generic_json(repo_root: str, launcher: str) -> str:
    return json.dumps(
        {
            "name": "matrixark",
            "transport": "stdio",
            "server": stdio_server(repo_root, launcher),
            "required_tools": [
                "matrixark_retrieve",
                "matrixark_ingest",
                "matrixark_feedback",
                "matrixark_session_commit",
            ],
        },
        indent=2,
        sort_keys=True,
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--client",
        choices=["codex", "claude", "cursor", "generic", "all"],
        default="all",
        help="Config snippet to print.",
    )
    parser.add_argument("--repo-root", default=DEFAULT_REPO_ROOT)
    parser.add_argument("--launcher", default=DEFAULT_LAUNCHER)
    args = parser.parse_args()

    generators = {
        "codex": ("# Codex config.toml", codex_toml),
        "claude": ("# Claude Desktop claude_desktop_config.json", claude_json),
        "cursor": ("# Cursor MCP config", cursor_json),
        "generic": ("# Generic MCP stdio config", generic_json),
    }
    selected = generators if args.client == "all" else {args.client: generators[args.client]}
    blocks: list[str] = []
    for _, (title, generator) in selected.items():
        blocks.append(title)
        blocks.append(generator(args.repo_root, args.launcher))
    print("\n".join(blocks))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
