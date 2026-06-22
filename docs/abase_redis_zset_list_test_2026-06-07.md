# ABase Redis LIST / ZSET Support Smoke

Date: 2026-06-07

## Summary

ABase has code-level support for Redis-style LIST and ZSET commands, and the current AWS smoke test confirmed those commands work through the Redis wire protocol when using the dual proxy binary.

The successful path is important:

- Register the proxy as `ABASE2_THRIFT_PROTOCOL`.
- Use the dual proxy binary.
- Send Redis RESP commands to the proxy data port.
- Use a table with `list_info.enable=true` and `zset_info.enable=true`.

Registering the proxy directly as `REDIS_PROTOCOL` failed in onebox because the master rejected it for the public consul path:

```text
only ABASE2_THRIFT_PROTOCOL is allowed with public consul
```

So the practical path today is "dual proxy registered as ABASE2, Redis commands accepted on the proxy port."

## Tendis Reference

Tencent Tendis is the right open-source comparison point for this area: it is a Redis-protocol-compatible persistent KV store backed by RocksDB. Its README says it is compatible with Redis protocol and commands, stores data through RocksDB, and exposes Redis clients as the access path.

The ABase similarity is Redis-style command/data-model support. The difference is that ABase is still being validated through its dual proxy path in our local/AWS setup, while Tendis is designed publicly as a Redis-compatible persistent storage system.

## Code Evidence

Local source path:

```text
/root/abase-wsl-build/abase
```

Redis command registration includes LIST and ZSET command names in:

```text
src/proxy/metrics.cc
```

Observed command families:

```text
ZSET:
zadd, zadd_with_limit, zincrby, zrem_by_member, zrem_by_rank, zrem_by_score,
zsetexpires, zdrop, zttl, zcount, zcard, zrank, zrevrank, zscore,
zrange_by_rank, zrange_by_score, zrevrange_by_rank, zrevrange_by_score

LIST:
lpush, lpushx, rpush, rpushx, lsetexpires, lindex, llen, lttl,
lexists, lrange, lpop, rpop, ltrim, ldrop
```

The table creation path enables both models in the onebox table definition:

```text
test/onebox/commands.py
zset_info: enable=true
list_info: enable=true
```

The Redis protocol test file also has dedicated LIST and ZSET coverage:

```text
test/onebox/test_proxy_redis_protocol.py
test_zset()
test_list()
```

## AWS Test Environment

Existing reused cluster only; no new AWS cluster was launched.

Terraform output at test time:

```text
meta/control/proxy/client node:
  instance: i-05f55360d92c43908
  private: 10.70.1.161
  public: 44.248.70.48

data01:
  instance: i-0cfbef56e86551535
  private: 10.70.1.214

data02:
  instance: i-04c93ad8271e5b64a
  private: 10.70.1.24
```

The test ran a self-contained local onebox on the existing meta EC2 node using staged ABase binaries:

```text
/opt/abase/abase-runtime/bin/abase-master
/opt/abase/abase-runtime/bin/abase-datanode
/opt/abase/abase-runtime/bin/abase-proxy.dual
```

SSM command id:

```text
95dfd2a3-b1b8-41e7-abac-4c83415f43bf
```

## Result

The focused Redis LIST/ZSET smoke passed.

Raw result:

```json
{
  "ok": true,
  "proxy": {
    "control_port": 5876,
    "ip": "127.0.0.1",
    "port": 5877,
    "protocol_type": "ABASE2_THRIFT_PROTOCOL"
  }
}
```

LIST commands tested:

```text
PING                         -> +PONG
DEL ts:list                  -> :1
LPUSH ts:list a              -> :1
LPUSH ts:list b              -> :2
RPUSH ts:list c              -> :3
LLEN ts:list                 -> :3
LRANGE ts:list 0 -1          -> b, a, c
LPOP ts:list                 -> b
RPOP ts:list                 -> c
```

ZSET commands tested:

```text
DEL ts:zset                  -> :1
ZADD ts:zset 1 alice         -> :1
ZADD ts:zset 2 bob           -> :1
ZADD ts:zset 3 carl          -> :1
ZCARD ts:zset                -> :3
ZSCORE ts:zset bob           -> 2.000000
ZRANGE ts:zset 0 -1 WITHSCORES
  -> alice 1.000000, bob 2.000000, carl 3.000000
ZREM ts:zset alice           -> :1
ZRANGE ts:zset 0 -1 WITHSCORES
  -> bob 2.000000, carl 3.000000
```

## Earlier Failed Attempts

Two false starts happened before the successful run:

1. One-node custom table setup created `default/onebox` with LIST and ZSET enabled, but table placement stayed in `TABLE_STATE_CREATING`.

   Root cause from master log:

   ```text
   failed to pick core, idc=local suitable_cores=0
   ```

   The test had simplified the topology too aggressively and queued replica creation before the master considered any core suitable.

2. Registering the proxy as `REDIS_PROTOCOL` failed:

   ```text
   only ABASE2_THRIFT_PROTOCOL is allowed with public consul
   ```

   The repo's own Redis tests use Redis commands against a proxy still registered as `ABASE2_THRIFT_PROTOCOL`, so the successful smoke followed that pattern.

## What This Proves

This proves:

- ABase supports LIST and ZSET data models in the current code.
- Redis RESP commands for representative LIST and ZSET operations work through the dual proxy path.
- The working onebox/AWS path is not pure `REDIS_PROTOCOL` registration; it is dual proxy plus ABASE2 registration.

This does not yet prove:

- Full Redis compatibility for every LIST/ZSET command variant.
- Large-scale LIST/ZSET throughput or latency.
- Production distributed Redis-mode registration across the two data nodes.
- ZSET/LIST behavior under failover or replication lag.

## Next Tests

Recommended follow-up tests:

- Run the repo's full `test_proxy_redis_protocol.py::test_zset` and `test_list` equivalent with the staged binaries.
- Add scale tests for:
  - large LIST length,
  - large ZSET cardinality,
  - `ZRANGE` by rank and score,
  - `ZREM` by member/rank/score,
  - concurrent LIST push/pop,
  - concurrent ZSET add/range/remove.
- Compare ABase behavior with Tendis/Kvrocks style RocksDB-backed Redis-compatible stores.
- Verify the production proxy registration path for Redis clients, not only onebox dual-proxy testing.
