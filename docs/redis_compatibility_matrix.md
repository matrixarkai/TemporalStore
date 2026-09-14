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
| String | `GET`, `SET`, `SETNX`, `SETEX`, `PSETEX`, `GETSET`, `GETDEL`, `GETEX`, `MGET`, `MSET`, `DEL`, `UNLINK`, `EXISTS`, `APPEND`, `STRLEN` | required | partial | `GET`, `SET key value`, `SET key value NX`, `SET key value XX`, `SET key value EX/PX`, `SET key value GET`, `SET key value KEEPTTL`, `SETNX`, `SETEX`, `PSETEX`, `GETSET`, `GETDEL`, `GETEX`, `MGET`, `MSET`, `DEL`, `UNLINK`, `EXISTS`, `APPEND`, and `STRLEN` are wired. `SET EX/PX` may be combined with `NX` or `XX`, and `KEEPTTL` with `NX`/`XX`/`GET` in any order; `KEEPTTL` beside `EX`/`PX` is a syntax error, since the two ask opposite things about one deadline. |
| Counter | `INCR`, `INCRBY`, `DECR`, `DECRBY` | required | wired | Wired through native string storage with integer parsing and overflow checks. |
| TTL | `EXPIRE`, `PEXPIRE`, `TTL`, `PTTL`, `PERSIST`, `GETEX` | required | partial | `EXPIRE`, `PEXPIRE`, `TTL`, `PTTL`, `PERSIST`, `SET EX/PX`, `SET KEEPTTL`, `SETEX`, `PSETEX`, and `GETEX` are wired. What each write does to an existing deadline is tabulated under "What a Write Does to the Deadline It Overwrites" below. Restart/failover TTL durability still belongs to the production gate. |
| Hash | `HSET`, `HSETNX`, `HMSET`, `HGET`, `HMGET`, `HGETALL`, `HKEYS`, `HVALS`, `HSTRLEN`, `HDEL`, `HEXISTS`, `HLEN`, `HINCRBY` | required | wired | Wired through native hash storage; smoke coverage includes conditional field creation, field listing, values, field length, integer updates, deletes, and missing-field behavior. |
| Set | `SADD`, `SREM`, `SMEMBERS`, `SCARD`, `SISMEMBER`, `SMISMEMBER`, `SPOP`, `SRANDMEMBER`, `SINTER`, `SUNION`, `SDIFF` | required | partial | Wired through native persistent-map set storage. Membership, pop/random-member, and set algebra are covered; random commands return deterministic members in the bridge smoke path to keep tests stable. |
| List | `LPUSH`, `RPUSH`, `LPUSHX`, `RPUSHX`, `LPOP`, `RPOP`, `LLEN`, `LINDEX`, `LRANGE`, `LTRIM`, `LSET`, `LREM` | required | partial | Wired through encoded values in native string storage. This gives real persistence with minimal storage churn, but does not yet provide a native list model or blocking list commands. |
| ZSet | `ZADD`, `ZINCRBY`, `ZREM`, `ZPOPMIN`, `ZPOPMAX`, `ZREMRANGEBYSCORE`, `ZREMRANGEBYRANK`, `ZCARD`, `ZSCORE`, `ZMSCORE`, `ZRANK`, `ZREVRANK`, `ZRANGE`, `ZREVRANGE`, `ZRANGEBYSCORE`, `ZREVRANGEBYSCORE`, `ZCOUNT` | required | partial | Wired through encoded values in native string storage. Supports score/member ordering, score mutation, pop min/max, range removal, rank, score, multi-score, count, remove, `WITHSCORES`, and score-range `LIMIT`; advanced zset options remain future work. |
| Scan | `SCAN`, `HSCAN`, `SSCAN`, `ZSCAN` | planned | unsupported | Needed for operational migration but can follow first smoke if documented. |
| Transactions | `MULTI`, `EXEC`, `DISCARD`, `WATCH` | planned | unsupported | Start same-partition only or return explicit unsupported errors. |
| Cluster | `CLUSTER SLOTS`, `CLUSTER NODES`, `MOVED`, `ASK` | planned | unsupported | Required only if exposing Redis Cluster wire compatibility. Proxy-hidden sharding can avoid this initially. |
| Pub/Sub | `PUBLISH`, `SUBSCRIBE`, `PSUBSCRIBE` | deferred | unsupported | Separate serving plane; not required for KV migration. |
| Streams | `XADD`, `XREAD`, `XGROUP`, `XACK` | deferred | unsupported | Separate from TemporalStore ingestion queues. |
| Scripting | `EVAL`, `EVALSHA`, functions | deferred | unsupported | High risk; defer until transaction semantics are stable. |
| Modules/advanced | GEO, HyperLogLog, bitmaps, module commands | deferred | unsupported | Do not claim full Redis for these until implemented and tested. Common GEO/HyperLogLog/bitmap probes are registered to return deterministic unsupported errors. |

## What a Write Does to the Deadline It Overwrites

Two surfaces reach the same string storage and they do NOT agree about this, so it is written
down here rather than left to be re-derived from whichever arm a reader opens first. The
disagreement is deliberate and each side is defensible on its own; what was wrong until now is
that neither was stated and callers could not find out without reading the engine.

The engine spells the choice as two commands, and which one a verb reaches for IS the semantic:

* `StringSetConditional` is the CLEARING write. With `ttl_ms` it arms that deadline, with
  `keep_ttl` it leaves the existing one alone, and with neither it removes the deadline.
* `StringSet` is the PRESERVING write. It never touches the expiry index at all.

### RESP surface

| Verb | Effect on an existing deadline | Why |
| --- | --- | --- |
| `SET key value` | **cleared** | The caller supplied the whole new value, so nothing of the old record survives it -- including its lifetime. Matches Redis. |
| `SET key value EX/PX n` | **replaced** with the new one | The caller named a deadline. |
| `SET key value KEEPTTL` | **kept, exactly** | The caller asked for the value to change and the countdown to continue. |
| `SETEX` / `PSETEX` | **replaced** | Same as `SET ... EX`. |
| `GETSET`, `MSET`, `MSETNX`, `SETNX` | **cleared** | Each is a `SET` in the shape of another answer; the caller still supplies the whole value. Corrected in #1713. |
| `APPEND`, `SETRANGE`, `INCR`, `INCRBY`, `DECR`, `DECRBY`, `INCRBYFLOAT` | **kept** | These derive the new value FROM the old one. They amend a record that is already there, so its lifetime is not theirs to reset -- a counter would otherwise restart its own countdown on every increment. |
| `GETEX key` | **kept** | No option words means no opinion about the deadline. |
| `GETEX key PERSIST` | **cleared** | Corrected in #1665. |
| `PERSIST` | **cleared** | That is the whole command. |

`KEEPTTL` may not be combined with `EX` or `PX`: they ask opposite things about one deadline,
so the pair is refused with `ERR syntax error` rather than resolved by an unwritten precedence
rule. It composes freely with `NX`, `XX` and `GET`, in any order.

### gRPC / SDK surface

`v1.StringSet` carries a single `ttl_ms` field, and zero is not a deadline:

* `ttl_ms > 0` becomes `StringSetEx` and **replaces** the deadline;
* `ttl_ms == 0` becomes `StringSet` and **keeps** it.

So the gRPC `Set` behaves like RESP `SET ... KEEPTTL`, not like RESP `SET`, and there is
currently **no way to clear a deadline through gRPC `Set`** -- `CommonPersist` is the command
for that. A caller moving between the two surfaces should not assume the plain set means the
same thing on both.

### Why they are not unified here

Making them agree is a bigger change than it looks, in either direction, which is why this
change documents the split and adds the missing option rather than closing it quietly:

* teaching gRPC `Set` to clear when `ttl_ms == 0` changes the meaning of a field already in the
  wire format for every existing client, with no spelling available for the old behaviour and no
  error raised at the moment the meaning flips;
* teaching `StringSet` itself to clear would reach `APPEND`, `SETRANGE` and the whole increment
  family, which share it deliberately -- every counter increment would silently reset the key's
  countdown. That is the wrong fix #1713 already named and guarded against.

Closing it properly means a third value on the wire -- an explicit "no deadline" distinct from
"no opinion" -- which is a proto change plus a version-negotiated default, and belongs with the
SDK contract rather than here.

## `EXPIREAT` Stores a Deadline Slightly Later Than the One Named

`EXPIREAT` / `PEXPIREAT` (and `GETEX EXAT` / `PXAT`) take an ABSOLUTE deadline, convert it to a
RELATIVE one at the RESP layer against that process's clock, and the shard then converts it back
to absolute against its own. Two readings of the clock, so what is stored is the deadline the
caller named plus however long the command took to get there. `PEXPIRETIME` adds a third reading
on the way back out.

Measured over 500 in-process rounds on a 16-core box at load average 11.5: **min 0 ms, median
0 ms, p99 1 ms, max 1 ms, mean 0.074 ms, with 37 of 500 rounds storing a deadline later than the
one named.** So the drift is real and it is sub-millisecond -- it is the cost of one command's
travel, not a clock-skew problem. The measurement is
`expireat_drift_measured`, kept `#[ignore]`d because its value is a property of how loaded the
machine is and gating on a threshold would be gating on the machine.

The DIRECTION, unlike the size, is not a timing measurement: the shard's clock is read strictly
after the RESP layer's, so a stored deadline is never EARLIER than the one named. A key can
therefore live a little longer than asked, never die sooner -- the safe direction -- and that is
what is pinned, by `an_absolute_deadline_is_stored_no_earlier_than_it_was_named`.

Removing the drift entirely needs an absolute-deadline command that does not round-trip through
a relative TTL -- a new engine command and a wire field, not a change to the RESP layer -- so it
is recorded here rather than half-done.

## Current Bridge Caveat

The native Redis data-command bridge currently requires the Redis service to have an explicitly loaded partition. `tools/run_redis_live_storage_smoke_ubuntu22.sh` validates that explicit local partition load plus STRING, TTL, HASH, SET, and deterministic unsupported-command paths work against live storage. If no partition is loaded through the Redis serving path, data commands fail fast with `ERR no partition loaded for Redis command serving`; they must not block. Metaserver/proxy-routed Redis serving still needs a production bootstrap path before this can be called full Redis migration support.

## Current Production-Ready Claim

The TemporalStore native Redis bridge is production-ready only for the documented first storage-backed subset:

- Connection/basic: `PING`, `ECHO`, `QUIT`, `AUTH`, `CLIENT SETNAME`, `CLIENT GETNAME`, `CLIENT ID`, `SELECT 0`, `INFO`, `COMMAND COUNT/DOCS/INFO`, `TYPE`
- String/common: `GET`, `SET key value`, `SET key value NX`, `SET key value XX`, `SET key value EX/PX`, `SET key value GET`, `SET key value KEEPTTL`, `SETNX`, `SETEX`, `PSETEX`, `GETSET`, `GETDEL`, `GETEX`, `MGET`, `MSET`, `DEL`, `UNLINK`, `EXISTS`, `APPEND`, `STRLEN`
- Counters: `INCR`, `INCRBY`, `DECR`, `DECRBY`
- TTL: `EXPIRE`, `PEXPIRE`, `TTL`, `PTTL`, `PERSIST`
- Hash: `HSET`, `HSETNX`, `HMSET`, `HGET`, `HMGET`, `HDEL`, `HEXISTS`, `HLEN`, `HGETALL`, `HKEYS`, `HVALS`, `HSTRLEN`, `HINCRBY`
- Set: `SADD`, `SREM`, `SMEMBERS`, `SCARD`, `SISMEMBER`, `SMISMEMBER`, `SPOP`, `SRANDMEMBER`, `SINTER`, `SUNION`, `SDIFF`
- List: `LPUSH`, `RPUSH`, `LPUSHX`, `RPUSHX`, `LPOP`, `RPOP`, `LLEN`, `LINDEX`, `LRANGE`, `LTRIM`, `LSET`, `LREM`
- ZSet: `ZADD`, `ZINCRBY`, `ZREM`, `ZPOPMIN`, `ZPOPMAX`, `ZREMRANGEBYSCORE`, `ZREMRANGEBYRANK`, `ZCARD`, `ZSCORE`, `ZMSCORE`, `ZRANK`, `ZREVRANK`, `ZRANGE`, `ZREVRANGE`, `ZRANGEBYSCORE`, `ZREVRANGEBYSCORE`, `ZCOUNT`
- Admin/bootstrap: explicit `PARTITION LOAD`/`PARTITION UNLOAD` for local Redis serving

The bridge must not claim full Redis compatibility yet. Unsupported command families return deterministic Redis errors rather than fake success. The current local bridge serializes backend Redis data-command execution while storage concurrency semantics are hardened; this favors correctness over peak Redis QPS.

Latest local collection-type gate:

```bash
RESULT_ROOT=/tmp/temporalstore-redis-sets-lists-zsets-20260611-001020 \
  REPEAT=2 BENCH_REQUESTS=20000 BENCH_CLIENTS=32 \
  tools/run_redis_production_gate_ubuntu22.sh
```

Result: PASS for the previous gate. Sets are storage-backed. Lists and sorted sets are storage-backed through encoded native string values while native LIST/ZSET modules remain future work. The compatibility smoke now also covers `GETEX`, extended `SET` forms, `HSETNX`, `HSTRLEN`, `SPOP`, `SRANDMEMBER`, `SINTER`, `SUNION`, `SDIFF`, `LPUSHX`/`RPUSHX`, `LSET`, `LREM`, `ZINCRBY`, `ZMSCORE`, `ZPOPMIN`/`ZPOPMAX`, reverse score ranges, and zset range removals.

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
