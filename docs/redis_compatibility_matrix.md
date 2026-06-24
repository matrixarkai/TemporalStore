# Redis Compatibility Matrix

Status values:

- `required`: must pass before MatrixDB/TemporalStore can claim Redis-compatible production migration for the first release.
- `planned`: important, but not part of the first production claim.
- `deferred`: advanced Redis behavior that should be called out as unsupported until implemented.

Current bridge status values:

- `wired`: command is registered and routed to native storage code.
- `partial`: some syntax or edge semantics are missing.
- `unsupported`: command returns a deterministic Redis error today.
- `not wired`: command is not implemented yet and must not be claimed.

| Family | Commands | First release status | Current bridge status | Production notes |
|---|---|---|---|---|
| Connection | `PING`, `ECHO`, `QUIT` | required | wired | `PING`, `ECHO`, and `QUIT` are wired for client compatibility. |
| Auth/client | `AUTH`, `CLIENT SETNAME`, `CLIENT GETNAME`, `CLIENT ID` | required | wired | `CLIENT SETNAME` is accepted, `GETNAME` returns null, and `ID` returns a deterministic compatibility value. |
| Metadata | `INFO`, `COMMAND`, `TYPE` | required | partial | `INFO` and `TYPE` are wired. `COMMAND COUNT/DOCS/INFO` return deterministic compatibility responses; full command metadata is still future work. |
| DB selection | `SELECT` | required | wired | `SELECT 0` is accepted. Non-zero DB indexes are rejected because isolation is namespace/table/scope based, not Redis logical DB based. |
| String | `GET`, `SET`, `SETNX`, `SETEX`, `PSETEX`, `GETSET`, `GETDEL`, `MGET`, `MSET`, `DEL`, `UNLINK`, `EXISTS`, `APPEND`, `STRLEN` | required | partial | `GET`, `SET key value`, `SET key value NX`, `SET key value XX`, `SETNX`, `SETEX`, `PSETEX`, `GETSET`, `GETDEL`, `MGET`, `MSET`, `DEL`, `UNLINK`, `EXISTS`, `APPEND`, and `STRLEN` are wired. More edge syntax such as extended `SET` options remains separate work. |
| Counter | `INCR`, `INCRBY`, `DECR`, `DECRBY` | required | wired | Wired through native string storage with integer parsing and overflow checks. |
| TTL | `EXPIRE`, `PEXPIRE`, `TTL`, `PTTL`, `PERSIST` | required | partial | `EXPIRE`, `PEXPIRE`, `TTL`, `PTTL`, and `PERSIST` are wired. `PSETEX` command syntax remains separate work. |
| Hash | `HSET`, `HMSET`, `HGET`, `HMGET`, `HGETALL`, `HKEYS`, `HVALS`, `HDEL`, `HEXISTS`, `HLEN`, `HINCRBY` | required | wired | Wired through native hash storage; edge semantics still need redis-py/conformance coverage. |
| Set | `SADD`, `SREM`, `SMEMBERS`, `SCARD`, `SISMEMBER`, `SMISMEMBER` | required | wired | Wired through native persistent-map set storage; local smoke covers duplicate add counts, membership, multi-membership, cardinality, members, and remove counts. |
| List | `LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `LLEN`, `LINDEX`, `LRANGE`, `LTRIM` | required | partial | Wired through encoded values in native string storage. This gives real persistence with minimal storage churn, but does not yet provide a native list model or every Redis list command. |
| ZSet | `ZADD`, `ZREM`, `ZCARD`, `ZSCORE`, `ZRANK`, `ZREVRANK`, `ZRANGE`, `ZREVRANGE`, `ZRANGEBYSCORE`, `ZCOUNT` | required | partial | Wired through encoded values in native string storage. Supports score/member ordering, range, rank, score, count, remove, `WITHSCORES`, and `ZRANGEBYSCORE LIMIT`; advanced options remain future work. |
| Scan | `SCAN`, `HSCAN`, `SSCAN`, `ZSCAN` | planned | unsupported | Needed for operational migration but can follow first smoke if documented. |
| Transactions | `MULTI`, `EXEC`, `DISCARD`, `WATCH` | planned | unsupported | Start same-partition only or return explicit unsupported errors. |
| Cluster | `CLUSTER SLOTS`, `CLUSTER NODES`, `MOVED`, `ASK` | planned | not wired | Required only if exposing Redis Cluster wire compatibility. Proxy-hidden sharding can avoid this initially. |
| Pub/Sub | `PUBLISH`, `SUBSCRIBE`, `PSUBSCRIBE` | deferred | unsupported | Separate serving plane; not required for KV migration. |
| Streams | `XADD`, `XREAD`, `XGROUP`, `XACK` | deferred | unsupported | Separate from TemporalStore ingestion queues. |
| Scripting | `EVAL`, `EVALSHA`, functions | deferred | unsupported | High risk; defer until transaction semantics are stable. |
| Modules/advanced | GEO, HyperLogLog, bitmaps, module commands | deferred | not wired | Do not claim full Redis for these until implemented and tested. |

## Current Bridge Caveat

The native Redis data-command bridge currently requires the Redis service to have an explicitly loaded partition. `tools/run_redis_live_storage_smoke_ubuntu22.sh` validates that explicit local partition load plus STRING, TTL, HASH, SET, and deterministic unsupported-command paths work against live storage. If no partition is loaded through the Redis serving path, data commands fail fast with `ERR no partition loaded for Redis command serving`; they must not block. Metaserver/proxy-routed Redis serving still needs a production bootstrap path before this can be called full Redis migration support.

## Current Production-Ready Claim

The TemporalStore native Redis bridge is production-ready only for the documented first storage-backed subset:

- Connection/basic: `PING`, `ECHO`, `QUIT`, `AUTH`, `CLIENT SETNAME`, `CLIENT GETNAME`, `CLIENT ID`, `SELECT 0`, `INFO`, `COMMAND COUNT/DOCS/INFO`, `TYPE`
- String/common: `GET`, `SET key value`, `SET key value NX`, `SET key value XX`, `SETNX`, `SETEX`, `PSETEX`, `GETSET`, `GETDEL`, `MGET`, `MSET`, `DEL`, `UNLINK`, `EXISTS`, `APPEND`, `STRLEN`
- Counters: `INCR`, `INCRBY`, `DECR`, `DECRBY`
- TTL: `EXPIRE`, `PEXPIRE`, `TTL`, `PTTL`, `PERSIST`
- Hash: `HSET`, `HMSET`, `HGET`, `HMGET`, `HDEL`, `HEXISTS`, `HLEN`, `HGETALL`, `HKEYS`, `HVALS`, `HINCRBY`
- Set: `SADD`, `SREM`, `SMEMBERS`, `SCARD`, `SISMEMBER`, `SMISMEMBER`
- Admin/bootstrap: explicit `PARTITION LOAD`/`PARTITION UNLOAD` for local Redis serving

The bridge must not claim full Redis compatibility yet. Unsupported command families return deterministic Redis errors rather than fake success. The current local bridge serializes backend Redis data-command execution while storage concurrency semantics are hardened; this favors correctness over peak Redis QPS.

Latest local collection-type gate:

```bash
RESULT_ROOT=/tmp/temporalstore-redis-sets-lists-zsets-20260611-001020 \
  REPEAT=2 BENCH_REQUESTS=20000 BENCH_CLIENTS=32 \
  tools/run_redis_production_gate_ubuntu22.sh
```

Result: PASS. Sets are storage-backed. Lists and sorted sets are now storage-backed through encoded native string values while native LIST/ZSET modules remain future work.

## Production Gate

Run the local production gate with:

```bash
tools/run_redis_production_gate_ubuntu22.sh
```

The gate builds the release server, audits no-op success paths, rejects `nullptr` Redis command handlers, runs the live storage smoke twice by default, runs the compatibility/pipeline/concurrency smoke, and runs a small `redis-benchmark` set/get profile when `redis-benchmark` is installed.

The first full Redis-compatible production claim still requires:

1. All `required` commands pass `tools/run_redis_compat_smoke_ubuntu22.sh`.
2. Redis client compatibility passes with at least `redis-cli` and `redis-py`.
3. TTL survives restart and replica/failover validation.
4. Pipelined command tests pass.
5. Scale smoke passes for STRING, HASH, LIST, and ZSET workloads.
6. Unsupported commands return deterministic Redis-style errors.
7. Prometheus metrics expose command QPS, latency, errors, connection count, rejected commands, and backend routing failures.
