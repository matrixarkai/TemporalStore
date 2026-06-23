# MatrixArk MCP Integration For Codex Desktop

This is the preferred integration path when Codex Desktop does not expose the
project hook approval UI.

## Local Codex Config

Add this server to `C:\Users\Deeproute\.codex\config.toml`:

```toml
[mcp_servers.matrixark]
command = 'wsl.exe'
args = [ '--cd', '/root/src/github-services/TemporalStore', '-e', 'bash', '-lc', 'exec tools/matrixark_mcp_cpp_server.sh' ]
startup_timeout_sec = 120
```

The local config on this machine has already been updated and a backup was
saved at:

```text
C:\Users\Deeproute\.codex\config.toml.bak-matrixark-mcp
```

Restart Codex Desktop after editing `config.toml` so it reloads MCP servers.

## What The Launcher Does

`tools/matrixark_mcp_cpp_server.sh` starts MatrixArk MCP over stdio with:

```text
MATRIXARK_MCP_BACKEND=temporalstore-direct
MATRIXARK_TEMPORALSTORE_METASERVER=127.0.0.1:18000
MATRIXARK_TEMPORALSTORE_NAMESPACE=deploy_ns
MATRIXARK_TEMPORALSTORE_TABLE=deploy_table
MATRIXARK_TEMPORALSTORE_PREFIX=matrixark:mcp:codex
TEMPORALSTORE_LIB=/root/src/github-services/TemporalStore/output-ubuntu22/release/sdk/lib/libbcache2.so
MATRIXARK_EMBEDDING_PROVIDER=oss
MATRIXARK_REQUIRE_OSS_EMBEDDINGS=1
MATRIXARK_UNDERSTANDING_PROVIDER=oss_encoder
MATRIXARK_REQUIRE_OSS_UNDERSTANDING=1
MATRIXARK_RETRIEVAL_TIMEOUT_MS=5000
```

This means MatrixArk MCP tools use the live C++ TemporalStore direct SDK and
OSS model-backed embeddings/query understanding by default.

## Tools Exposed To Codex

The MCP server exposes MatrixArk tools including:

```text
matrixark_ingest
matrixark_batch_extract
matrixark_session_commit
matrixark_refresh_summaries
matrixark_retrieve
matrixark_feedback
matrixark_replay
matrixark_admin_create_account
matrixark_admin_create_user
matrixark_admin_create_api_key
matrixark_admin_list_api_keys
```

Codex can call these tools directly instead of relying on lifecycle hooks.

## Smoke Test

From PowerShell:

```powershell
$req = '{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}' + "`n"
Set-Content -NoNewline -Encoding ascii "$env:TEMP\matrixark-mcp-smoke.jsonl" $req
wsl --cd /root/src/github-services/TemporalStore -e bash -lc 'timeout 30s tools/matrixark_mcp_cpp_server.sh --line-json < /mnt/c/Users/Deeproute/AppData/Local/Temp/matrixark-mcp-smoke.jsonl | head -c 500'
```

Expected output starts with:

```json
{"id": 1, "jsonrpc": "2.0", "result": {"tools": [{"description": "Ingest chat...
```

## Usage Pattern

For a prompt:

```text
1. Codex calls matrixark_retrieve(raw query + scope + token budget).
2. Codex combines returned ContextPack with local context.
3. After useful output or tool result, Codex calls matrixark_ingest.
4. At task/session boundary, Codex calls matrixark_session_commit.
```

This is more explicit than hidden hooks: Codex decides when to retrieve, ingest,
or commit. MatrixArk still owns extraction, entities, segments, summaries,
indexes, embeddings, temporal scoring, and ContextPack construction.
