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
| Metadata | `INFO`, `COMMAND`, `TYPE` | required | partial | `INFO` and `TYPE` are wired. In open-source builds, `INFO stats` exposes `redis_surface:trimmed_open_source_context_feature_frequency`, `redis_surface_schema`, command-count, and blocked-family count fields. In open-source C++ builds, plain `COMMAND` advertises only the trimmed 47-command string/hash surface, `COMMAND COUNT` derives from that same table, and `COMMAND INFO` returns minimal metadata for those commands; full Redis command metadata is still future work. |
| DB selection | `SELECT` | required | wired | `SELECT 0` is accepted. Non-zero DB indexes are rejected because isolation is namespace/table/scope based, not Redis logical DB based. |
| String | `GET`, `SET`, `SETNX`, `SETEX`, `PSETEX`, `GETSET`, `GETDEL`, `GETEX`, `MGET`, `MSET`, `DEL`, `UNLINK`, `EXISTS`, `APPEND`, `STRLEN` | required | partial | `GET`, `SET key value`, `SET key value NX`, `SET key value XX`, `SET key value EX/PX`, `SET key value GET`, `SETNX`, `SETEX`, `PSETEX`, `GETSET`, `GETDEL`, `GETEX`, `MGET`, `MSET`, `DEL`, `UNLINK`, `EXISTS`, `APPEND`, and `STRLEN` are wired. `SET EX/PX` combined with `NX/XX` is rejected until the native string module exposes an atomic conditional set-with-ttl primitive. |
| Counter | `INCR`, `INCRBY`, `DECR`, `DECRBY` | required | wired | Wired through native string storage with integer parsing and overflow checks. |
| TTL | `EXPIRE`, `PEXPIRE`, `TTL`, `PTTL`, `PERSIST`, `GETEX` | required | partial | `EXPIRE`, `PEXPIRE`, `TTL`, `PTTL`, `PERSIST`, `SET EX/PX`, `SETEX`, `PSETEX`, and `GETEX` are wired. Restart/failover TTL durability still belongs to the production gate. |
| Hash | `HSET`, `HSETNX`, `HMSET`, `HGET`, `HMGET`, `HGETALL`, `HKEYS`, `HVALS`, `HSTRLEN`, `HDEL`, `HEXISTS`, `HLEN`, `HINCRBY`, `HINCRBYFLOAT` | required | partial | `HSET`, `HSETNX`, `HMSET`, `HGET`, `HMGET`, `HGETALL`, `HKEYS`, `HVALS`, `HSTRLEN`, `HDEL`, `HEXISTS`, `HLEN`, and integer `HINCRBY` are wired in both open-source bridges. Rust also wires `HINCRBYFLOAT`; the C++ Redis bridge must not claim it until a native handler is added. Smoke coverage keeps `HINCRBYFLOAT` behind `REDIS_EXPECT_HINCRBYFLOAT=1` for C++ live runs. |
| Feature model | `FAPPEND`, `FAPPENDPOLICY`, `FQUERY`, `FQUERYFILTER`, `FQUERYFILTERSTR`, `FAGG` | required | wired | TemporalStore feature APIs are exposed as explicit data-model commands rather than pretending to be generic Redis collections. These support timestamped feature writes, policy writes, range query, filtered query, and aggregate query. |
| Frequency-control model | `RISKINCR`, `RISKINCROPT`, `RISKCHANGE`, `RISKCOUNT`, `RISKQUERY`, `RISKDETAIL`, `RISKHSET`, `HCHANGE`, `HQUERY`, `HSETANDGET`, `CPCSET`, `CPCSETANDGET`, `FOLSET`, `FOLQUERY` | required | wired | Frequency-cap/risk-window commands remain data-model specific. Debug-only inspection commands are not part of the open-source surface. |
| Set/List/ZSet clone APIs | `SADD`, `SREM`, `SMEMBERS`, `LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `ZADD`, `ZRANGE`, `ZRANGEBYSCORE`, and related collection commands | deferred | private/unsupported in open-source surface | These compatibility handlers may exist internally, but open-source production builds do not advertise or allow them. Context/feature/frequency use native data-model APIs instead of encoded Redis collection clones. |
| Narrow hash scan | `HSCAN` | required | wired | `HSCAN` is allowed only as a single-hash/narrow helper. Broad keyspace `SCAN`, `SSCAN`, and `ZSCAN` are not part of the open-source production surface. |
| Transactions | `MULTI`, `EXEC`, `DISCARD`, `WATCH` | planned | unsupported | Start same-partition only or return explicit unsupported errors. |
| Cluster | `CLUSTER SLOTS`, `CLUSTER NODES`, `MOVED`, `ASK` | planned | unsupported | Required only if exposing Redis Cluster wire compatibility. Proxy-hidden sharding can avoid this initially. |
| Pub/Sub | `PUBLISH`, `SUBSCRIBE`, `PSUBSCRIBE` | deferred | unsupported | Separate serving plane; not required for KV migration. |
| Streams | `XADD`, `XREAD`, `XGROUP`, `XACK` | deferred | unsupported | Separate from TemporalStore ingestion queues. |
| Scripting | `EVAL`, `EVALSHA`, functions | deferred | unsupported | High risk; defer until transaction semantics are stable. |
| Modules/advanced | GEO, HyperLogLog, bitmaps, module commands | deferred | unsupported | Do not claim full Redis for these until implemented and tested. Common GEO/HyperLogLog/bitmap probes are registered to return deterministic unsupported errors. |

## Current Bridge Caveat

The native Redis data-command bridge currently requires the Redis service to have an explicitly loaded partition. `tools/run_redis_live_storage_smoke_ubuntu22.sh` validates that explicit local partition load plus STRING, TTL, HASH, feature/frequency data-model commands, and deterministic unsupported-command paths work against live storage. If no partition is loaded through the Redis serving path, data commands fail fast with `ERR no partition loaded for Redis command serving`; they must not block. Metaserver/proxy-routed Redis serving still needs a production bootstrap path before this can be called full Redis migration support.

## Current Production-Ready Claim

The TemporalStore native Redis bridge is production-ready only for the documented first storage-backed subset:

- Connection/basic: `PING`, `ECHO`, `QUIT`, `AUTH`, `CLIENT SETNAME`, `CLIENT GETNAME`, `CLIENT ID`, `SELECT 0`, `INFO`, `COMMAND COUNT/DOCS/INFO`, `TYPE`
- String/common: `GET`, `SET key value`, `SET key value NX`, `SET key value XX`, `SET key value EX/PX`, `SET key value GET`, `SETNX`, `SETEX`, `PSETEX`, `GETSET`, `GETDEL`, `GETEX`, `MGET`, `MSET`, `DEL`, `UNLINK`, `EXISTS`, `APPEND`, `STRLEN`
- Counters: `INCR`, `INCRBY`, `DECR`, `DECRBY`
- TTL: `EXPIRE`, `PEXPIRE`, `TTL`, `PTTL`, `PERSIST`
- Hash: `HSET`, `HSETNX`, `HMSET`, `HGET`, `HMGET`, `HDEL`, `HEXISTS`, `HLEN`, `HGETALL`, `HKEYS`, `HVALS`, `HSTRLEN`, `HINCRBY`, `HSCAN`; `HINCRBYFLOAT` is a Rust-only/opt-in bridge capability until the C++ Redis bridge adds a native handler
- Feature model: `FAPPEND`, `FAPPENDPOLICY`, `FQUERY`, `FQUERYFILTER`, `FQUERYFILTERSTR`, `FAGG`
- Frequency-control model: `RISKINCR`, `RISKINCROPT`, `RISKCHANGE`, `RISKCOUNT`, `RISKQUERY`, `RISKDETAIL`, `RISKHSET`, `HCHANGE`, `HQUERY`, `HSETANDGET`, `CPCSET`, `CPCSETANDGET`, `FOLSET`, `FOLQUERY`

Rust's RESP bridge also advertises additional normal string/TTL helpers in
the trimmed surface: `MSETNX`, `TOUCH`, `EXPIREAT`, `PEXPIREAT`,
`EXPIRETIME`, `PEXPIRETIME`, `GETRANGE`, `SETRANGE`, and `INCRBYFLOAT`.
These are still part of the basic data-model surface; they are not generic
collection clones, server-admin APIs, scripting, streams, pub/sub, or debug
commands. The C++ Redis bridge currently exposes a narrower
basic/string/hash subset in open-source builds and reports `COMMAND COUNT=47`;
feature and frequency-control commands are Rust bridge APIs.

Narrow `HSCAN` is intentionally limited to a single hash key and is implemented by
fetching that hash's fields through native hash storage, applying optional
`MATCH`/`COUNT` paging, and returning Redis-style cursor pages. It must not be
treated as broad keyspace scan support.

Open-source production builds do not claim generic Redis SET/LIST/ZSET compatibility or Redis server-configuration APIs. `SADD`, `LPUSH`, `ZADD`, `SCAN`, `PARTITION`, `CONFIG`, `DBSIZE`, `IPS*`, scripting, streams, pub/sub, GEO, HyperLogLog, and bitmap commands must return deterministic unsupported/open-surface errors.

The bridge must not claim full Redis compatibility yet. Unsupported command families return deterministic Redis errors rather than fake success. When `TEMPORALSTORE_OPEN_SOURCE_SURFACE=1` or `TS_OPEN_SOURCE_SURFACE=1`, Rust filters both execution and `COMMAND` advertising to the trimmed production data-model surface. C++ open-source builds likewise derive `COMMAND`, `COMMAND COUNT`, and `COMMAND INFO` from one canonical trimmed descriptor table. The current local bridge serializes backend Redis data-command execution while storage concurrency semantics are hardened; this favors correctness over peak Redis QPS.

The canonical machine-readable contract for this surface is `compat/redis_open_source_surface_manifest.json`; its `allowed_surface_families` field is the authoritative list of public Redis-style families for this open-source build. The validator `tools/validate_open_source_surface.py` checks the C++ `COMMAND` descriptor table, Rust allowlist, blocked families, required blocked-command smoke samples, helper commands, and docs against that manifest so the public API cannot silently drift.

Latest open-source surface gate:

```bash
python3 tools/validate_open_source_surface.py
cargo check -p temporalstore-rust --lib
```

Result expectation: the public Rust surface keeps string/common, hash, feature, and frequency-control commands; `COMMAND` does not advertise `SADD`, `LPUSH`, `ZADD`, broad `SCAN`, `PARTITION`, `CONFIG`, `DBSIZE`, stale feature aliases such as `FADD`, or internal/debug families such as `IPS*`/`RISKDEBUG`.

## Production Gate

Run the local production gate with:

```bash
tools/run_redis_production_gate_ubuntu22.sh
```

The gate first runs `tools/validate_open_source_surface.py` against the canonical manifest, runs `tools/validate_matrixobjectstore_names.py` to keep retired object-store naming out of the open-source surface, and copies `redis_open_source_surface_manifest.json`, `redis_open_source_surface_validation.txt`, and `matrixobject_name_validation.txt` into `RESULT_ROOT`, then builds the release server, audits no-op success paths, rejects `nullptr` Redis command handlers, runs the live storage smoke twice by default, runs the trimmed compatibility/pipeline/concurrency smoke, and runs a small `redis-benchmark` profile when `redis-benchmark` is installed. The benchmark profile emits the manifest-declared required CSV artifacts for `SET/GET`, `HSET`, `HGET`, `HINCRBY`, `INCR`, and `EXPIRE`, plus the manifest-declared opt-in `HINCRBYFLOAT` artifact only when `REDIS_EXPECT_HINCRBYFLOAT=1`; it then writes `redis-benchmark-summary.json` with Redis surface identity, schema, and manifest hash plus command count, per-command request/sec min/max/avg values, and overall request/sec min/max/avg values across the trimmed benchmark set, plus `min_overall_qps_threshold` when `REDIS_BENCH_MIN_OVERALL_QPS` is configured. This covers the string/common, hash, counter, TTL, `COMMAND COUNT` bounds derived from the manifest C++ command count plus Rust extra data-model/helper commands, and `INFO stats` surface-identity fields used to prove the trimmed API contract for context, feature, and frequency-control data models. C++ live smoke and the shared compatibility benchmark keep `HINCRBYFLOAT` opt-in with `REDIS_EXPECT_HINCRBYFLOAT=1` until the C++ Redis bridge adds a native handler; Rust-focused runs can enable that flag when validating Rust's float hash path. The production gate forces `REDIS_COMPAT_SURFACE=trimmed` and `REDIS_EXPECT_UNSUPPORTED_COLLECTIONS=1`, so it fails if collection-clone commands such as `SADD`, `LPUSH`, or `ZADD` are accepted by an open-source production build. Use `REDIS_COMPAT_SURFACE=full` only outside this gate for private/broad Redis compatibility experiments.

The canonical manifest declares `surface: trimmed_open_source_context_feature_frequency` and the required/opt-in benchmark command sets. The benchmark summary copies that exact identity, expected/executed command coverage, and expected command count so perf artifacts cannot be confused with private/full Redis compatibility experiments.

The production claim for the trimmed Redis-style API requires:

1. All `required` string/common, hash, feature, and frequency-control commands pass focused smoke coverage.
2. Redis client compatibility passes with at least `redis-cli` and `redis-py` for the supported subset.
3. TTL survives restart and replica/failover validation.
4. Pipelined command tests pass for the supported subset.
5. Scale smoke passes for STRING, HASH, feature, and frequency-control workloads.
6. Unsupported collection-clone and advanced commands return deterministic Redis-style errors.
7. Prometheus metrics expose command QPS, latency, errors, connection count, rejected commands, and backend routing failures.
