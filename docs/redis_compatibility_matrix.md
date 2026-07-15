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
| Metadata | `INFO`, `COMMAND` | required | partial | `INFO stats` exposes `redis_surface:trimmed_open_source_context_feature_control`, `redis_surface_schema`, command-count, and blocked-family count fields. In open-source C++ builds, plain `COMMAND` advertises only the trimmed 16-command string/hash/TTL surface; Feature and Control State commands are Rust bridge APIs. |
| DB selection | `SELECT` | excluded | unsupported in open-source surface | Isolation is namespace/table/scope based, not Redis logical DB based. |
| String | `GET`, `SET`, `DEL`, `EXISTS` | required | wired | First release keeps only the minimal string commands needed by context, feature, and Control State plumbing. |
| Counter | `INCR`, `INCRBY`, `DECR`, `DECRBY` | excluded | unsupported in open-source surface | Counters are represented by Control State instead of generic Redis integer keys. |
| TTL | `EXPIRE`, `TTL` | required | wired | First release keeps only minimal key lifetime support. Restart/failover TTL durability still belongs to the production gate. |
| Hash | `HSET`, `HGET`, `HGETALL`, `HDEL`, `HEXISTS`, `HLEN` | required | wired | First release keeps only minimal hash commands. Hash counters, hash scans, key/val listing helpers, and float increments are not public APIs. |
| Feature + FeatureAggregate | `FAPPEND`, `FAPPENDPOLICY`, `FQUERY`, `FQUERYFILTER`, `FQUERYFILTERSTR`, `FAGG` | required | wired | TemporalStore feature APIs are exposed as explicit commands rather than pretending to be generic Redis collections. Feature stores timestamped observations; FeatureAggregate computes mature exact serving aggregates over those observations: `count`, `sum`, `min`, `max`, `avg`, `first`, and `latest`. High-cardinality sketches such as `distinct_count`, `top_k`, `heavy_hitters`, `hll`, histograms, and percentiles are intentionally gated. |
| Control State | `CONTROLINCR`, `CONTROLINCROPT`, `CONTROLCHANGE`, `CONTROLCOUNT`, `CONTROLQUERY`, `CONTROLDETAIL`, `CONTROLHSET` | required | wired | Control State stores fast-changing serving signals such as counters, caps, quotas, pacing, eligibility, suppression, and risk-control state. Legacy `RISKINCR`, `RISKINCROPT`, `RISKCHANGE`, `RISKCOUNT`, `RISKQUERY`, `RISKDETAIL`, `RISKHSET`, `HCHANGE`, `HQUERY`, and `HSETANDGET` remain compatibility aliases during migration. |
| Set/List/ZSet clone APIs | `SADD`, `SREM`, `SMEMBERS`, `LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `ZADD`, `ZRANGE`, `ZRANGEBYSCORE`, and related collection commands | deferred | private/unsupported in open-source surface | These compatibility handlers may exist internally, but open-source production builds do not advertise or allow them. Context/feature/frequency use native capability APIs instead of encoded Redis collection clones. |
| Narrow hash scan | `HSCAN` | excluded | unsupported in open-source surface | Use `HGETALL` for the minimal hash read path. Broad keyspace `SCAN`, `SSCAN`, and `ZSCAN` are not part of the open-source production surface. |
| Transactions | `MULTI`, `EXEC`, `DISCARD`, `WATCH` | planned | unsupported | Start same-partition only or return explicit unsupported errors. |
| Cluster | `CLUSTER SLOTS`, `CLUSTER NODES`, `MOVED`, `ASK` | planned | unsupported | Required only if exposing Redis Cluster wire compatibility. Proxy-hidden sharding can avoid this initially. |
| Pub/Sub | `PUBLISH`, `SUBSCRIBE`, `PSUBSCRIBE` | deferred | unsupported | Separate serving plane; not required for KV migration. |
| Streams | `XADD`, `XREAD`, `XGROUP`, `XACK` | deferred | unsupported | Separate from TemporalStore ingestion queues. |
| Scripting | `EVAL`, `EVALSHA`, functions | deferred | unsupported | High risk; defer until transaction semantics are stable. |
| Modules/advanced | GEO, HyperLogLog, bitmaps, module commands | deferred | unsupported | Do not claim full Redis for these until implemented and tested. Common GEO/HyperLogLog/bitmap probes are registered to return deterministic unsupported errors. |

## Current Bridge Caveat

The native Redis data-command bridge currently requires the Redis service to have an explicitly loaded partition. `tools/run_redis_live_storage_smoke_ubuntu22.sh` validates that explicit local partition load plus STRING, TTL, HASH, Feature, and Control State commands, and deterministic unsupported-command paths work against live storage. If no partition is loaded through the Redis serving path, data commands fail fast with `ERR no partition loaded for Redis command serving`; they must not block. Metaserver/proxy-routed Redis serving still needs a production bootstrap path before this can be called full Redis migration support.

## Current Production-Ready Claim

The TemporalStore native Redis bridge is production-ready only for the documented first storage-backed subset:

- Connection/basic: `PING`, `AUTH`, `INFO`, `COMMAND COUNT/DOCS/INFO`
- String minimal: `GET`, `SET key value`, `DEL`, `EXISTS`
- TTL: `EXPIRE`, `TTL`
- Hash minimal: `HSET`, `HGET`, `HDEL`, `HEXISTS`, `HLEN`, `HGETALL`
- Feature + FeatureAggregate: `FAPPEND`, `FAPPENDPOLICY`, `FQUERY`, `FQUERYFILTER`, `FQUERYFILTERSTR`, `FAGG`
- Control State: `CONTROLINCR`, `CONTROLINCROPT`, `CONTROLCHANGE`, `CONTROLCOUNT`, `CONTROLQUERY`, `CONTROLDETAIL`, `CONTROLHSET`; legacy aliases `RISKINCR`, `RISKINCROPT`, `RISKCHANGE`, `RISKCOUNT`, `RISKQUERY`, `RISKDETAIL`, `RISKHSET`, `HCHANGE`, `HQUERY`, `HSETANDGET`

The C++ Redis bridge exposes the 16-command minimal string/hash/TTL subset in open-source builds and reports `COMMAND COUNT=16`; Feature and Control State commands are Rust bridge APIs. `CONTROL*` is the preferred spelling for the first-release serving-signal surface; `RISK*` command names remain compatibility aliases.

`HSCAN` is not part of the first-release open-source Redis surface; use `HGETALL` for the minimal hash read path. Broad keyspace scan support remains excluded.

Open-source production builds do not claim generic Redis SET/LIST/ZSET compatibility or Redis server-configuration APIs. `SADD`, `LPUSH`, `ZADD`, `SCAN`, `PARTITION`, `CONFIG`, `DBSIZE`, scripting, streams, pub/sub, GEO, HyperLogLog, and bitmap commands must return deterministic unsupported/open-surface errors.

The bridge must not claim full Redis compatibility yet. Unsupported command families return deterministic Redis errors rather than fake success. When `TEMPORALSTORE_OPEN_SOURCE_SURFACE=1` or `TS_OPEN_SOURCE_SURFACE=1`, Rust filters both execution and `COMMAND` advertising to the trimmed production capability surface. C++ open-source builds likewise derive `COMMAND`, `COMMAND COUNT`, and `COMMAND INFO` from one canonical trimmed descriptor table. The current local bridge serializes backend Redis data-command execution while storage concurrency semantics are hardened; this favors correctness over peak Redis QPS.

The canonical machine-readable contract for this surface is `compat/redis_open_source_surface_manifest.json`; its `allowed_surface_families` field is the authoritative list of public Redis-style families for this open-source build. The validator `tools/validate_open_source_surface.py` checks the C++ `COMMAND` descriptor table, Rust allowlist, blocked families, required blocked-command smoke samples, helper commands, and docs against that manifest so the public API cannot silently drift.

Latest open-source surface gate:

```bash
python3 tools/validate_open_source_surface.py
cargo check -p temporalstore-rust --lib
```

Result expectation: the public Rust surface keeps minimal string/hash/TTL commands plus context, Feature/FeatureAggregate, and Control State capabilities; `COMMAND` does not advertise `SADD`, `LPUSH`, `ZADD`, broad `SCAN`, `PARTITION`, `CONFIG`, `DBSIZE`, stale feature aliases such as `FADD`, gated feature sketches such as `distinct_count`/`hll`/`top_k`, or internal/private command families.

## Production Gate

Run the local production gate with:

```bash
tools/run_redis_production_gate_ubuntu22.sh
```

The gate first runs `tools/validate_open_source_surface.py` against the canonical manifest, runs `tools/validate_matrixobjectstore_names.py` to keep retired object-store naming out of the open-source surface, and copies `redis_open_source_surface_manifest.json`, `redis_open_source_surface_validation.txt`, and `matrixobject_name_validation.txt` into `RESULT_ROOT`, then builds the release server, audits no-op success paths, rejects `nullptr` Redis command handlers, runs the live storage smoke twice by default, runs the trimmed compatibility/pipeline/concurrency smoke, and runs a small `redis-benchmark` profile when `redis-benchmark` is installed. The benchmark profile emits the manifest-declared required CSV artifacts for `SET/GET`, `HSET`, `HGET`, and `EXPIRE`; it then writes `redis-benchmark-summary.json` with Redis surface identity, schema, and manifest hash plus command count, per-command request/sec min/max/avg values, and overall request/sec min/max/avg values across the trimmed benchmark set, plus `min_overall_qps_threshold` when `REDIS_BENCH_MIN_OVERALL_QPS` is configured. This covers the minimal string, hash, TTL, `COMMAND COUNT` bounds derived from the manifest C++ command count plus Rust extra capability commands, and `INFO stats` surface-identity fields used to prove the trimmed API contract for context, feature, and Control State. The production gate forces `REDIS_COMPAT_SURFACE=trimmed` and `REDIS_EXPECT_UNSUPPORTED_COLLECTIONS=1`, so it fails if collection-clone commands such as `SADD`, `LPUSH`, or `ZADD` are accepted by an open-source production build. Use `REDIS_COMPAT_SURFACE=full` only outside this gate for private/broad Redis compatibility experiments.

Each live storage smoke writes `redis-live-storage-smoke-summary.json`, proving the trimmed `INFO stats` identity, command count, blocked-family count, and unsupported collection-clone checks for that run. The production gate rolls those per-run summaries into `redis-live-storage-smoke-rollup.json`.

The production gate also writes `redis-production-benchmark-rollup.json`, a top-level rollup across repeated live benchmark runs, including the enforced `min_overall_qps_threshold`. It also writes `redis-production-gate-summary.json`, which ties the manifest hash, open-source surface validator, MatrixObject naming guard, live-smoke rollup, and optional benchmark rollup into one top-level evidence artifact. The canonical manifest declares `surface: trimmed_open_source_context_feature_control`, the required/opt-in benchmark command sets, and `production_gate_artifacts`, so both API scope and required production evidence are manifest-governed. The benchmark summary copies that exact identity, expected/executed command coverage, and expected command count so perf artifacts cannot be confused with private/full Redis compatibility experiments.

The production claim for the trimmed Redis-style API requires:

1. All `required` minimal string/hash/TTL commands plus context, feature, and Control State capabilities pass focused smoke coverage.
2. Redis client compatibility passes with at least `redis-cli` and `redis-py` for the supported subset.
3. TTL survives restart and replica/failover validation.
4. Pipelined command tests pass for the supported subset.
5. Scale smoke passes for STRING, HASH, Feature, and Control State workloads.
6. Unsupported collection-clone and advanced commands return deterministic Redis-style errors.
7. Prometheus metrics expose command QPS, latency, errors, connection count, rejected commands, and backend routing failures.
