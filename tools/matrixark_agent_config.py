#!/usr/bin/env python3
"""Generate MatrixArk agent integration snippets.

The MatrixArk MCP server is the common boundary for Codex, Claude Desktop,
Cursor, and vertical AI agents.  This helper emits copy-pasteable config for
the common clients without requiring each integration doc to drift.
"""

from __future__ import annotations

import argparse
import json


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


def claude_code_json(repo_root: str, launcher: str) -> str:
    return json.dumps(
        {
            "matrixark": stdio_server(repo_root, launcher),
            "usage": "Register this stdio server with the Claude Code MCP configuration path used by your installation.",
        },
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


def agent_policy_text() -> str:
    return """# MatrixArk Agent Policy

Use MatrixArk as durable remote context. Keep using your native local context
for currently open files, active buffers, terminal output, and immediate tool
results.

Before answering, call matrixark_retrieve when the request may depend on:
- prior decisions, approvals, incidents, debugging attempts, or user memory
- shared resources, runbooks, skills, team/project history, or cross-device context
- current-state facts that may have superseded older facts

Pass:
- query: the raw user request
- scope: account_id/tenant_id plus user_id and preferably session_id
- max_context_tokens: the remote context budget
- local_context: short refs or summaries of current files/buffers when useful

After answering or using tools, call matrixark_ingest or matrixark_feedback when
the turn produced durable information:
- accepted or rejected refs
- final answer summary
- correction, confirmation, decision, approval, commitment, or tool outcome

At task/session boundaries, call matrixark_session_commit so MatrixArk can run
one-pass batch extraction over the same-session buffer.
"""


def hook_examples_text(repo_root: str) -> str:
    hook = f"{repo_root}/tools/matrixark_agent_hook.py"
    return f"""# MatrixArk Popular Agent Hook Examples

# Claude Code-style hook command
python3 {hook} --agent claude --event UserPromptSubmit
python3 {hook} --agent claude --event PostToolUse
python3 {hook} --agent claude --event Stop

# Codex hook command
python3 {hook} --agent codex --event UserPromptSubmit
python3 {hook} --agent codex --event PostToolUse
python3 {hook} --agent codex --event Stop

# Cursor / Windsurf / Cline / Continue fallback hook command
# Use this when the client has an external hook/task runner. Otherwise use MCP.
python3 {hook} --agent cursor --event UserPromptSubmit
python3 {hook} --agent windsurf --event UserPromptSubmit
python3 {hook} --agent cline --event UserPromptSubmit
python3 {hook} --agent roo --event UserPromptSubmit
python3 {hook} --agent continue --event UserPromptSubmit

# Other popular coding and orchestration agents
python3 {hook} --agent opencode --event UserPromptSubmit
python3 {hook} --agent openclaw --event UserPromptSubmit
python3 {hook} --agent aider --event UserPromptSubmit
python3 {hook} --agent gemini --event UserPromptSubmit
python3 {hook} --agent qwen-code --event UserPromptSubmit
python3 {hook} --agent autogen --event UserPromptSubmit
python3 {hook} --agent langgraph --event UserPromptSubmit
python3 {hook} --agent crewai --event UserPromptSubmit
python3 {hook} --agent llamaindex --event UserPromptSubmit
python3 {hook} --agent semantic-kernel --event UserPromptSubmit
python3 {hook} --agent dify --event UserPromptSubmit
python3 {hook} --agent n8n --event UserPromptSubmit

# Generic stdin payload smoke
echo '{{"prompt":"Alice approved the GPU request.","session_id":"demo-thread"}}' | \\
  python3 {hook} --agent generic --event UserPromptSubmit --backend local
"""


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--client",
        choices=["codex", "claude", "claude-code", "cursor", "generic", "policy", "hooks", "all"],
        default="all",
        help="Config snippet to print.",
    )
    parser.add_argument("--repo-root", default=DEFAULT_REPO_ROOT)
    parser.add_argument("--launcher", default=DEFAULT_LAUNCHER)
    args = parser.parse_args()

    generators = {
        "codex": ("# Codex config.toml", codex_toml),
        "claude": ("# Claude Desktop claude_desktop_config.json", claude_json),
        "claude-code": ("# Claude Code MCP server entry", claude_code_json),
        "cursor": ("# Cursor MCP config", cursor_json),
        "generic": ("# Generic MCP stdio config", generic_json),
    }
    if args.client == "policy":
        print(agent_policy_text())
        return 0
    if args.client == "hooks":
        print(hook_examples_text(args.repo_root))
        return 0
    selected = generators if args.client == "all" else {args.client: generators[args.client]}
    blocks: list[str] = []
    for _, (title, generator) in selected.items():
        blocks.append(title)
        blocks.append(generator(args.repo_root, args.launcher))
    print("\n".join(blocks))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
