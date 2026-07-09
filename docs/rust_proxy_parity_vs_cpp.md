# Rust Proxy Parity Vs C++ TemporalStore

## Goal

Rust TemporalStore should expose the same production-facing proxy surface that C++ clients expect, so MatrixArk can route through a long-lived Rust proxy without falling back to process-per-operation tools.

## Implemented ProxyService Aliases

The Rust proxy accepts the C++-style `ProxyService` routes below and maps them to native TemporalStore commands:

| Route | Native command |
| --- | --- |
| `/ProxyService/Get` | `StringGet` |
| `/ProxyService/Set` | `StringSet` |
| `/ProxyService/Delete` | `CommonDelete` |
| `/ProxyService/Expire` | `CommonExpire` |
| `/ProxyService/Ttl` | `CommonTtl` |
| `/ProxyService/HGet` | `HashGet` |
| `/ProxyService/HSet` | `HashSet` |
| `/ProxyService/HDel` | `HashDelete` |
| `/ProxyService/HMGet` | `HashMultiGet` |
| `/ProxyService/HMSet` | `HashMultiSet` |
| `/ProxyService/HGetAll` | `HashGetAll` |
| `/ProxyService/HLen` | `HashLen` |
| `/ProxyService/SAdd` | `SetAdd` |
| `/ProxyService/SMembers` | `SetMembers` |
| `/ProxyService/FeatureAdd` | `FeatureAppend` |
| `/ProxyService/RiskHset` | `RiskSet` |

Generic table command routes remain available:

- `/ProxyService/ExecuteCmd`
- `/ProxyService/BatchExecuteCmd`
- `/ProxyService/TableExecuteCmd`
- `/ProxyService/BatchExecuteTableCmd`

## Rust SDK Proxy Helpers

The Rust SDK proxy client now exposes direct-method parity for common C++ client operations:

- `hset`
- `hget`
- `hdel`
- `sadd`
- `smembers`
- `delete_object`
- `expire`
- `ttl`
- `matrixark_batch_append_records`
- `matrixark_scan_candidates_request_json`
- `matrixark_retrieve_context_pack_request_json`
- `parse_matrixark_retrieve_context_pack_response`

These helpers call the `/ProxyService/...` routes and parse native proxy responses.

## MatrixArk Path

For MatrixArk production workloads:

1. Python MCP remains the API/auth/model orchestration layer.
2. Python sends storage and retrieval work to C++ direct SDK or Rust proxy/native SDK.
3. Rust proxy owns long-lived routing, command execution, batching routes, metrics, and readiness.
4. Process-per-operation record-log paths are compatibility/debug only, not the production path.

## Remaining Work

- Keep direct Rust SDK/C ABI parity as an embedded/local optimization alongside the proxy path.
