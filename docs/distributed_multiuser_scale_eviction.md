# Distributed multi-user scale validation (with eviction + promotion)

`distributed_multiuser_scale_harness` stress-tests the TemporalStore storage service in a local
setup with **several users, a large corpus each, partitioned under a per-user namespace/table**,
and a **small per-datanode memory cache** so that the working set is forced through the full
storage tiering: `memory -> disk-cache -> block-store` (eviction) on writes, and
`block-store -> memory` (promotion) on cold reads.

## What it validates

For each of several iterations at increasing scale:

- **Multi-user partitioning** — each user gets namespace `user{n}` / table `corpus`, hashed
  (FNV-1a) to a shard in `[1, shards]`; shard `s` is hosted on datanode `(s-1) % datanodes`.
  Datanodes are independent `TemporalEngine`s, run in parallel (one scoped thread per datanode).
- **Write/read correctness** — every string record is read back and compared byte-for-byte to
  what was written; each per-user feature series is range-queried and checked for exact count +
  tail value. Any drift counts as a mismatch (must be `0`).
- **Partition isolation** — a key one past a user's corpus must not resolve in that user's
  namespace (must be `0` violations).
- **Eviction actually happened** — aggregate `cache.memory_evictions > 0` and the disk-cache
  byte count grew (data spilled out of the tiny memory tier).
- **Cold-read promotion actually happened** — aggregate `block_store.reads > 0` (reads served
  from cold storage) and `cache.memory_fills > 0` (promoted back into memory), while the
  read-back still validated correct.

The harness exits non-zero if any invariant fails on any iteration.

## Results (local, release build)

Two runs, six iterations total, all **PASS** with **0 mismatches / 0 isolation violations**:

| datanodes | shards | users | writes (str+feat) | reads validated | memory evictions | disk cache | cold-read promotions |
|-----------|--------|-------|-------------------|-----------------|------------------|-----------:|----------------------|
| 4  | 16 | 16 | 9.6k  | 6.4k  | 12,840  | 1.7 MB | 6,432   |
| 4  | 16 | 25 | 24k   | 16k   | 32,067  | 4.3 MB | 16,050  |
| 4  | 16 | 40 | 61k   | 41k   | 82,112  | 11 MB  | 41,120  |
| 8  | 32 | 32 | 28.8k | 19.2k | 24,370  | 5.2 MB | 19,264  |
| 8  | 32 | 51 | 73.4k | 49k   | 85,985  | 13 MB  | 49,113  |
| 8  | 32 | 81 | 187k  | 124k  | 237,055 | 34 MB  | 124,821 |

Totals: **256k reads validated, 0 correctness mismatches, 0 partition-isolation violations**, with
every iteration forcing real memory→disk eviction and disk→memory cold-read promotion.

## How to run

```bash
cargo run --release -p temporalstore-rust --bin distributed_multiuser_scale_harness -- \
  --datanodes 8 --shards 32 --users 32 \
  --string-records-per-user 600 --feature-points-per-user 300 \
  --iterations 3 --memory-bytes 524288 --value-bytes 96
```

Or via the wrapper: `tools/run_distributed_multiuser_scale.sh`.

Flags: `--users`, `--string-records-per-user`, `--feature-points-per-user`, `--datanodes`,
`--shards` (must be `>= datanodes`), `--memory-bytes` (per-datanode memory-cache budget — smaller
forces more eviction), `--value-bytes`, `--iterations`, `--scale-growth-percent` (per-iteration
growth of users + corpus), `--root`.

## Scope

This exercises the **real datanode storage engine** (MultiLayerCache memory/disk tiering, block
store, packed pages, per-shard state) and the **namespace/table → shard → datanode partition
routing** in-process, across parallel datanodes. It is the storage-and-partitioning half of the
distributed service. The **metaserver/proxy TCP process wiring** (route lookup, registration,
heartbeat, proxy forwarding) is covered separately by `distributed_raft_harness`,
`ops_scale_readiness_harness`, and the proxy tests; a future extension can drive this same
multi-user corpus over the real metaserver+proxy+datanode processes.
