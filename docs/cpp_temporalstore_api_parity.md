# C++ TemporalStore API Parity

Reference sources checked:

- `/home/vj/src/temporalstore/src/protocol/*module.proto`
- `/home/vj/src/temporalstore/src/extension/*/interface.proto`
- local TemporalStore deep-dive docs under `Documents/Codex/2026-05-10/bytekv-in-local-vs-etcd`

## Implemented In Rust

Common:

- `DEL_OBJECT` -> `CommonDelete`
- `EXPIRE` -> `CommonExpire`
- `TTL` -> `CommonTtl`
- Redis-compatible `EXISTS`

String:

- `SET` -> `StringSet`
- `SETEX` -> `StringSetEx`
- `GET` -> `StringGet`
- Redis-compatible `MGET`, `MSET`, `GETDEL`, `DEL`, `EXPIRE`, `PEXPIRE`, `TTL`, `PTTL`

Hash:

- `SET` -> `HashSet`
- `GET` -> `HashGet`
- `DEL` -> `HashDelete`
- `MGET` -> `HashMultiGet`
- `MSET` -> `HashMultiSet`
- `GETALL` -> `HashGetAll`
- `LEN` -> `HashLen`
- `INCRBY` -> `HashIncrBy`, including C++-style rejection of non-integer values and i64 overflow
- Redis-compatible `HSET`, `HGET`, `HDEL`, `HMGET`, `HMSET`, `HGETALL`, `HLEN`, `HINCRBY`

Set:

- `SADD` -> `SetAdd`
- `SMEMBERS` -> `SetMembers`
- Rust additionally has `SetRemove` / Redis `SREM`

Feature:

- `ADD` -> `FeatureAppend`
- `QUERY` -> `FeatureQuery`
- `AGGQUERY` -> `FeatureAggQuery`
- `REPLACE` -> `FeatureReplace`
- `DEL` -> `FeatureDelete`

Runtime/control surface:

- load/unload shard
- config get/set
- stats/info
- membership update
- page/index stream read and scan
- in-process Raft behavior model
- S3-compatible snapshot store abstraction

Redis operational/admin compatibility:

- `AUTH`
- `BGSAVE`
- `CONFIG GET`
- `CONFIG SET`
- `CONFIG REWRITE`
- `INFO`
- `SLAVEOF`
- `PARTITION LOAD`
- `PARTITION UNLOAD`
- `PARTITION INFO`
- CRC64 slot/hash helpers through `PSLOTHASHKEY`, `PCLUSTERKEYSLOT`, and `PCLUSTERHASH`

## Partially Implemented

IPS:

- Rust has simplified `IpsAdd` and `IpsQueryLast`.
- C++ IPS has `ADD`, `BATCH_QUERY`, `REMOVE`, `LOAD`, `DEL`, plus rich feature-stat filters,
  data ranges, action/table dimensions, server aggregation, idempotence, and snap info.

Risk:

- Rust has simplified `RiskIncrement` and `RiskCount`.
- C++ risk has `HSET`, `HQUERY`, `CPCSET`, `CPCQUERY`, `FOLSET`, `FOLQUERY`,
  `HSETANDGET`, `FOLSETANDGET`, `CPCSETANDGET`, and `MANAGER`.
- Missing C++ semantics include precision buckets, windows, count/min/max/change operators,
  CPC/list detail, first/last tracking, TTL per risk object, and manager/debug operations.

Feature:

- Rust supports point append/query/replace/delete and simple count/sum/min/max aggregation.
- C++ feature API includes richer `FeaturePoint` structure with nested point arrays, time ranges,
  write policy, and aggregate query behavior. Rust currently stores one value per timestamp.

String:

- C++ `SetRequest` has `nx_flag` and `xx_flag`; Rust now supports equivalent
  `StringSetConditional` plus Redis-compatible `SET NX/XX GET EX/PX`.

## Missing Production Runtime Features

The C++ deep dive describes a full serving engine. The Rust rewrite does not yet have:

- metaserver namespace/table/partition-set topology model
- slot hashing and routing compatible with C++ `crc64 >> 34`
- brpc/thrift SDK compatibility
- partition worker pools
- hot object manager with dirty slot tracking
- append-only oplog with reclaim boundaries
- background merged slot/page dump scheduler
- ByteStore stream backend
- blockcache/mtcache-compatible cache engine
- readonly replica replay from primary
- production ByteRaft/OpenRaft integration
- heartbeat/load report to metaserver
- quota/admission control
- full metrics server and dashboards

## Current Conclusion

The Rust repo now covers the main simple module APIs: common, string, hash, set, feature, plus
simplified IPS and risk. It is not yet feature-complete versus the full C++ TemporalStore product,
mainly because IPS, Risk, routing/topology, oplog/dump/load, and production replication are still
large subsystems rather than small API aliases.
