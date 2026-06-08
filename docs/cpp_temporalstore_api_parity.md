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
- write-policy add -> `FeatureAppendWithPolicy` with `upsert`, `insert_if_absent`, and
  `replace_existing`
- `QUERY` -> `FeatureQuery`
- `AGGQUERY` -> `FeatureAggQuery`
- `REPLACE` -> `FeatureReplace`
- `DEL` -> `FeatureDelete`
- Typed client coverage: `feature_append`, `feature_append_with_policy`, `feature_query`,
  `feature_replace`, `feature_delete`, `feature_agg_query`
- RESP coverage: `FAPPEND`, `FAPPENDPOLICY`, `FQUERY`, `FREPLACE`, `FDEL`, `FAGG`

Sequence:

- add rows -> `SequenceAdd`
- query rows -> `SequenceQuery`
- batch query rows -> `SequenceBatchQuery`
- Typed client coverage: `sequence_add`, `sequence_query`, `sequence_batch_query`

IPS:

- `ADD` -> `IpsAdd`
- dimension/idempotent add -> `IpsAddWithOptions`
- query last -> `IpsQueryLast`
- range query -> `IpsQueryRange`
- dimension-filtered range query -> `IpsQueryRangeWithOptions`
- batch query last -> `IpsBatchQueryLast`
- remove timestamp -> `IpsRemove`
- delete key -> `IpsDelete`
- count range -> `IpsCount`
- load point snapshot -> `IpsLoad`
- snapshot range -> `IpsSnapshot`
- range stats -> `IpsStat`
- named dimension filter -> `IpsFilter`
- Typed client coverage: `ips_add`, `ips_add_with_options`, `ips_query_last`,
  `ips_query_range`, `ips_query_range_with_options`, `ips_batch_query_last`,
  `ips_remove`, `ips_delete`, `ips_count`, `ips_load`, `ips_snapshot`, `ips_stat`,
  `ips_filter`
- RESP coverage: `IPSADD`, `IPSADDOPT`, `IPSQUERYLAST`, `IPSQUERYRANGE`,
  `IPSQUERYRANGEOPT`, `IPSBATCHQUERYLAST`, `IPSREMOVE`, `IPSDEL`, `IPSCOUNT`,
  `IPSLOAD`, `IPSSNAPSHOT`, `IPSSTAT`, `IPSFILTER`

Risk:

- increment -> `RiskIncrement`
- precision/TTL increment -> `RiskIncrementWithOptions`
- count/sum window -> `RiskCount`
- aggregate query -> `RiskQuery` with `sum`, `min`, `max`, `first`, `last`, and `events`
- detail list -> `RiskDetail`
- Typed client coverage: `risk_increment`, `risk_increment_with_options`, `risk_count`,
  `risk_query`, `risk_detail`
- RESP coverage: `RISKINCR`, `RISKINCROPT`, `RISKCOUNT`, `RISKQUERY`, `RISKDETAIL`

Runtime/control surface:

- load/unload shard
- config get/set
- stats/info
- membership update with C++-style stale global/unit version rejection and local active-membership reporting
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

- Rust covers add, dimension/idempotent add, query-last, range query, dimension-filtered range
  query, batch query-last, remove timestamp, delete key, count range, local load, range snapshot,
  stats, and named filter, with typed client and RESP coverage.
- C++ IPS additionally has richer server aggregation and production snap metadata beyond the local
  Rust snapshot/stat surface.

Risk:

- Rust covers increment, precision/TTL increment, count, sum/min/max/first/last/event aggregation,
  and detail lists, with typed client and RESP coverage.
- C++ risk has `HSET`, `HQUERY`, `CPCSET`, `CPCQUERY`, `FOLSET`, `FOLQUERY`,
  `HSETANDGET`, `FOLSETANDGET`, `CPCSETANDGET`, and `MANAGER`.
- Missing C++ semantics include CPC/list-specific behavior and manager/debug operations.

Feature:

- Rust supports point append/query/replace/delete, write-policy append, and simple
  count/sum/min/max aggregation.
- C++ feature API includes richer `FeaturePoint` structure with nested point arrays, time ranges,
  and richer aggregate query behavior. Rust currently stores one value per timestamp.

String:

- C++ `SetRequest` has `nx_flag` and `xx_flag`; Rust now supports equivalent
  `StringSetConditional` plus Redis-compatible `SET NX/XX GET EX/PX`.

## Missing Production Runtime Features

The C++ deep dive describes a full serving engine. The Rust rewrite does not yet have:

- full metaserver namespace/table/partition-set placement hierarchy
- slot hashing and routing compatible with C++ `crc64 >> 34`; Rust also has an opt-in C++
  `PartitionId` bit-layout helper for table partition ids
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

The Rust repo now covers the main simple module APIs: common, string, hash, set, feature,
sequence, and the implemented IPS/Risk subset with typed client and RESP coverage. It is not yet
feature-complete versus the full C++ TemporalStore product, mainly because exact C++ proto
semantics, routing/topology, oplog/dump/load, and production replication are still large subsystems
rather than small API aliases.
