# TemporalStore

**A distributed, time-aware serving engine, written in Rust.**
One durable store for the read paths that sit in front of a model or a request: ranked context,
long sequences, aggregated features, frequency caps, and control state.
Apache-2.0 · self-hostable · no vector database · [temporalstore.ai](https://temporalstore.ai)

📊 **Benchmarks & docs — [temporalstore.ai/benchmarks.html](https://temporalstore.ai/benchmarks.html)**

Keys are time-ordered, writes are append-structured, and every workload below is answered from one
temporal index rather than from several stores that each keep their own idea of time. It runs
locally with one Docker command and scales out to a replicated, shared-storage cluster.

**Every figure on this page is measured, and carries its denominator and its conditions** — store
size, routing range, payload size, node count. Each one names what reproduces it, and all of them
were re-measured on `main` for this document except the one that says it was not. Where a limit is
real it is stated plainly, and [What this store is bad at](#what-this-store-is-bad-at) is a section
rather than a footnote.

---

## Is this the store for your problem?

| You are building | This store | Read on |
|---|---|---|
| **Event sequences per entity that grow without bound** — clickstreams, session histories, interaction logs — read as a recent window | Keeps one sorted array per key, so a window read is a binary search plus the points in the window and does **not** grow with the tail. Retains by **count** (10,000 points a series by default), trimmed oldest-first on every write. No per-point age policy. | [§1](#1-long-sequences-per-entity) |
| **Many millions of distinct entities, each with few values** | **39.4 B of resident index per key** at 20,000 keys in one shard, falling from 86.3 B/key at 2,000 as per-bucket rows amortise. A 64 MiB per-shard resident bound is reached at roughly 1.7 M keys. | [§2](#2-high-cardinality-features) |
| **Rollups over a window** — counts, sums, min/max/avg | Exact, recomputed from the points on each read; an exact incremental ladder at 1 s / 1 m / 1 h / 1 d is available for sum-family counters. **No decayed scores, no top-k, no percentiles.** | [§3](#3-aggregated-features) |
| **Plain online feature serving** — point reads at serving latency | Addresses from a DRAM-resident index, bytes from a memory cache tier, disk only on a miss. Reads are cheap and local; writes pay a consensus round trip. Measured 21x apart at p50 on one box. | [§4](#4-online-feature-serving) |
| **Context for an LLM application** under a token budget | Retrieve, rank, and pack to `max_prompt_tokens`; the prompt is charged first and blocks are taken greedily in ranked order, with what did not fit recorded. Ranking is lexical plan score plus cosine over per-node vectors. **No ANN index** — scoring is linear in the candidate set the caller names. | [§5](#5-context-for-llm-applications) |
| **Frequency caps, quotas, pacing, blobs, stream ingestion** | Single-key atomic counters with windowed sums, a multipart blob path with range reads, and batch/stream ingestion with a dead-letter export. | [§6](#6-what-else-the-engine-actually-does) |

Underneath all six: an append-structured block store, no GC pauses, crash-safe reload from its own
persistence, and a format stamp that refuses an image it cannot read rather than guessing.

---

## 1. Long sequences per entity

A series is a **sorted owned array per key**, not a per-key tree: a one-point series costs one
allocation, and both ends of a window are located by binary search, so a read costs
`O(log n)` plus the points inside the window. The cost of a long tail falls on **memory and
retention**, not on the read.

**Windowing.** Every series read names a window. `FeatureQuery` / `SequenceQuery` take
`start_ms`, `end_ms` and a `count`; `SequenceBatchQuery` carries several windows in one request;
`FeatureQueryFiltered` adds `AND`-ed predicates over four fixed row fields.

**Retention is by count, and it is enforced on the write path.** `feature_max_size` defaults to
**10,000 points a series**, trimmed oldest-first on every append, and it is also the default *and
the maximum* read bound — a `count` above it is clamped, so a long window cannot return more than
the retained length. One append is hard-capped at 100,000 points. Whole-key TTL
(`CommonExpire` / `CommonTtl`) is evaluated lazily on read and by a background sweep bounded at
128 in-memory buckets and 8 on-store buckets a round, so expiry never becomes an unbounded scan.

**What the tail costs.** The term that grows with the shard is the resident index. Measured on the
shipped routing range `0..1023`, one shard, single-page records:

| keys in the shard | resident index | per key |
|---:|---:|---:|
| 2,000 | 172,672 B | 86.3 B |
| 8,000 | 404,992 B | 50.6 B |
| 20,000 | 788,224 B | 39.4 B |

*`data_node::tests::evict_live::what_a_default_eviction_threshold_has_to_clear`.*

**Limits.** Retention is by count or by whole-key TTL — there is **no per-point age policy** and no
per-point deadline. Row filters are evaluated *after* decoding each candidate row, so a wide
filtered window pays for every row in it. The only destructive series verbs are a window
overwrite (`FeatureReplace`) and a whole-key delete (`FeatureDelete`).

## 2. High-cardinality features

This is the case where per-key overhead decides whether the design is affordable, so the figure is
given rather than an adjective.

**Resident index per key: 39.4 B at 20,000 keys in one shard** (shipped routing range `0..1023`,
single-page string records). It *falls* with key count — 86.3 B/key at 2,000, 50.6 at 8,000 —
because the per-bucket rows are shared by every key that lands in the bucket. Quoting a per-key
figure without its key count therefore means nothing here.

**The resident bound.** `index_resident_limit_bytes` ships at **64 MiB a shard**. At 39.4 B/key
that bound is reached at roughly **1.7 M keys in one shard**, which is the scale it is a bound at.

**What the routing range buys.** A page's bucket is `start + hash(object_key) % (end - start + 1)`,
so the range sets the modulus and therefore how many pages share a bucket. On the shipped
`0..1023`, per **routed record**, incompressible values, one shard — two allocator columns, because
the rounding is exactly what a change of container shape moves:

| routed records | `0..1023` asked / charged | whole keyspace asked / charged | pages a bucket holds (p50 / max) |
|---:|---:|---:|---:|
| 4,000 | 94.8 / 107.0 B/rec | 187.3 / 197.1 B/rec | 4 / 8 |
| 40,000 | **52.8 / 54.0 B/rec** | 186.5 / 198.1 B/rec | 39 / 50 |

*`engine::tests::routing_range_default::both_byte_columns_and_the_allocations_a_record_at_every_candidate_range`,
854 s. "Asked" is what the allocator was asked for; "charged" reads `malloc_usable_size`. The
whole-keyspace column is the arm where every key lands alone in its own bucket by construction.*

**At a matched denominator** — same corpus, same 256 B values, one shard, weighing the whole
resident shard state rather than the index alone — the shipped range costs **316.3 B/key at 8,000
keys (marginal 274.5 B/key)** against **472.6 B/key at 2,000 keys (marginal 468.0 B/key)** on the
whole keyspace. *(`where_the_resident_index_bytes_go_on_both_routing_ranges`.)*

**The per-key figure is a sawtooth, and that is a property of the measurement.** The hash table
behind a per-key map is a step function of the key count, so dividing by keys gives a sawtooth that
doubles the moment the table does: the string table measures **46.9 B/key at 1,792 keys and 93.3
B/key at 1,800** — 1.99x for eight more keys — while the whole resident index steps 414.1 → 460.7
B/key (1.113x), diluted by the per-bucket rows that do not move.
*(`the_per_key_index_figure_is_a_sawtooth`.)*

**Limits.** A store records the routing range it was built on and **refuses** to load under a
different one, and refuses an unstamped store rather than guessing; the range is a decision taken
before the first ingest, not afterwards. Only new stores take the shipped default.

## 3. Aggregated features

`FeatureAggQuery` takes one aggregator over a window: `sum`, `avg`, `min`, `max`, `first`, `last`,
`count`. Values are `i64` throughout, so `avg` is an integer division and there are no float
aggregates. The same shapes exist for control-state counters, where `"at most N in the last H
hours"` is a single round trip.

**By default an aggregate is recomputed from the points in the window on every read** — the window
is ranged, each point decoded, and the aggregator folded over the result. Exact, and linear in the
window.

**Kept current without recomputing: the rollup ladder.** For sum-family counters an optional index
maintains exact per-bucket sums at **1 s → 1 m → 1 h → 1 d**, folded forward on each counter write,
so a wide window reads a few coarse cells plus the sub-second edges instead of every raw bucket. A
fully covered window is **bit-identical** to the raw scan; partial edges fall back to the
authoritative series; the index is derived, never serialized, rebuilt whenever its recorded length
disagrees with the series, and dropped on delete, expiry or reconcile.

**Batching amortises only the fixed part of a record, and the fixed part is small.** An index-log
delta record measures 63.0 B for one item, 55.1 B/item at 8 and 54.3 B/item at 32, with a marginal
**54.00 B/item over 1..8 and 54.08 B/item over 8..32** — three sizes, because two points fit any
line. About 9 B a record is shareable; the rest travels with the item.
*(`index_log::tests::what_an_index_item_costs_per_append_at_three_batch_sizes`.)*

**Limits, and they matter.** The ladder is **off by default**, and on the feature path it serves
only a literal `sum` with no `count`. There is **no decayed score** anywhere in the engine — no
time-decay function and no decay aggregator; recency enters retrieval as a tiebreak, not as a
curve. There is no top-k, percentile, histogram or distinct aggregator on the feature surface, no
multi-aggregate request, and an **unknown aggregator name returns the point count rather than an
error**. On the feature path the ladder's first read after an append re-derives its numeric view
from the whole series, so an alternating append-and-aggregate workload pays a full-series decode
per aggregate.

## 4. Online feature serving

**The tiers.** A point read resolves its address in the **DRAM-resident index** — in-process, no
I/O, no cache call — then fetches bytes from the **memory cache tier**, falling through to a local
**block slab** (or to the log, for a block still resident there) only on a miss. The resident index
holds addresses, never values. A **disk tier** exists but is only switched on where it spans a real
distance: with shared storage the durable copy is on another machine, so a local disk tier closes
milliseconds; with consensus replication the durable copy is this node's own disk, where the tier
would be a third copy of bytes the operating system already serves from the same device, so it is
off. The backend is resolved before the engine is built, and publishes the reason it chose what it
chose. The budget is `TS_CACHE_MEMORY_BYTES` (16 MiB default), and it is the only one:
`TS_PAGE_INDEX_CACHE_BYTES` and `TS_BLOCK_INDEX_CACHE_BYTES` (64 MiB each) are published in a
node's tuning report and bound nothing, because the resident index named above takes no cache
call and so has no cache to bound.

**Pages are packed**, so one window read of adjacent instants typically decodes one block, and a
multi-point read memoizes each block for the duration of the query.

**The read/write asymmetry, measured.** A three-node cluster, consensus write path against replica
reads served from local state, 504 proposals and 44 sampled reads in a 2,018 ms run, one process,
one box (release build, WSL Ubuntu 22.04):

| | p50 | p95 | p99 | max |
|---|---:|---:|---:|---:|
| write, through consensus (504 samples) | **2,652 µs** | 8,144 µs | 12,004 µs | 20,200 µs |
| read, from a replica's local state (44 samples) | **126 µs** | 537 µs | 1,025 µs | 1,291 µs |

**21x apart at p50**, and the reason is structural rather than tunable: a write is replicated to and
committed on a majority before it is acknowledged, while a read takes a lock and applies the command
against the state the node already holds. The same run reported `replication_healthy`, a maximum
replica lag of **0**, a full error budget, **one failover and two membership changes exercised
during the workload**, and all three voters converged on commit index 504.
*(`scale_harness --nodes 3 --string-ops 400 --hash-ops 100 --sequence-keys 4 --sequence-len 100
--read-sample-every 10`.)*

**What one append's envelope costs.** Over incompressible payloads from 0 to 8,192 B under a
7-byte key, a framed write-ahead record fits

```
one local WAL record = 1.0004 * payload + 36.4 B      worst residual 3.4 B
```

so the marginal cost of a payload byte is a payload byte, and the fixed cost is 36.4 B. What that
means per write depends entirely on the payload:

| payload | user bytes | framed bytes | framed / user |
|---:|---:|---:|---:|
| 0 | 7 | 33 | 4.71x |
| 128 | 135 | 167 | 1.24x |
| 512 | 519 | 551 | 1.06x |
| 8,192 | 8,199 | 8,231 | 1.00x |

*(`wal::tests::one_record_envelope_terms::what_one_local_wal_record_costs`. The regime is asserted
at every point: a control sweep over repeated bytes fits a slope of −0.0021 instead of 1.0004,
which is the compressor and not the log, so any envelope figure taken on compressible values is
measuring zstd.)*

**Write amplification by payload size.** Amplification here is dominated by the fixed per-record
**envelope**, so it dilutes as the payload grows. Measured end to end — every durable byte a write
causes, over the bytes it carries — 4,000 records a rung, genuinely incompressible payloads, one
shard:

| value | user bytes | blocks | index | total written |
|---:|---:|---:|---:|---:|
| 64 B | 356,000 | 0.85x | 3.39x | **4.25x** |
| 256 B | 1,124,000 | 0.95x | 1.79x | **2.74x** |
| 1,024 B | 4,196,000 | 0.99x | 1.17x | **2.16x** |
| 4,096 B | 16,484,000 | 1.00x | 1.06x | **2.06x** |

The payload itself is written about once at every rung — the block column stays between 0.85x and
1.00x — so the whole excess is envelope: the record's framing plus the index log that every write
appends to. It is worst exactly where the payload is smallest. Once maintenance has reclaimed the
logs the *retained* index settles at roughly 740–780 KB at every rung whatever the payload, which
is a smaller and more flattering number for the same workload; write amplification conventionally
means the first.
*(`engine::tests::part4::what_a_write_costs_across_every_subsystem`.)*

**None of that is replication, and replication is the other cost.** Every amplification figure this
engine publishes is **per node**: replication is a mode here (consensus or shared storage), not a
factor, so no replica count appears in any of them. Three voters means three physical copies of
every committed byte — a deliberate cost that buys durability, and arithmetic rather than a
measurement. The two are quoted apart so neither is mistaken for the other. On the per-node side,
the index log costs **1.528x** its own image — log written over index image — at 2,000 keys × 256 B
in one shard. *(`the_amplification_set_in_the_published_format`.)*

**And a warning about one of this engine's own exported ratios.** Rewriting the same 2,000 keys ×
256 B five times, the block-granular tally reads **5.000x** write and **5.000x** space
amplification — one extra copy a rewrite, which is correct — while the `space_amplification` figure
the engine *publishes* reads **1.000x throughout**, because its denominator is slab-granular and
cannot see a rewrite that lands inside a slab. Space does not come back quickly either: a compacted
slab waits out a 60-minute quarantine before release, so space amplification cannot fall within the
quarantine age of the rewrite that caused it. Do not read a published 1.000x as evidence about a
rewriting workload. *(`the_amplification_set_under_rewrite`, 22.5 s.)*

**Maintenance is nine stages in a fixed order**, run as one round by a scheduler: `prepare`,
`reclaim_wal`, `expire`, `evict`, `reclaim_memory`, `reclaim_page`, `reclaim_index`,
`compact_pages`, `reap_metrics`. The order is load-bearing — a blanket cache invalidation ahead of
eviction zeroes the number eviction's own gate reads — and a test asserts the stages that ran did
so in that order. Some are pressure-gated and legitimately skip a round.

By ablation — timing the round, then timing it again with one stage disabled at a time — a whole
round measured **685 ms on a 5,000-object shard and 2,269 ms on a 20,000-object one** (release
build, one shard, 96-byte values). Two stages dominate at both sizes: reclaiming the write-ahead
log (427 ms of the 685, then 1,419 ms of the 2,269) and reclaiming the index log (418 ms, then
1,208 ms).

Read the per-stage column as an order of magnitude, not a constant. A repeat of the same
measurement on the same box put the round at 602 ms and 1,863 ms and moved individual stages by up
to 50%, and in each run at least one stage ablated to a *negative* cost — meaning its own cost sits
under this instrument's run-to-run noise. The two dominant stages are the same in both runs, and
that is the part of the table worth planning against. The ablation also covers eight of the nine
stages: eviction is never switched off, so the round has no per-stage figure for it.
*(`data_node::tests::part3::what_each_maintenance_stage_costs`, about 290 s a run.)*

The **resident bound** is the other half of maintenance: `index_resident_limit_bytes` caps the
resident index at **64 MiB a shard**, which at the 39.4 B/key of [§2](#2-high-cardinality-features)
is reached at roughly 1.7 M keys in one shard. A shard under the bound declines to evict, and that
is correct — a cap firing at a few percent of its limit would be a thrash rather than a bound.

## 5. Context for LLM applications

This is the workload the project was built around, and it is the one with the most surface.

**Ingest → extract → retrieve → pack.** Retrieval takes a tenant, a time window, a set of candidate
node hashes and a query, and returns ranked `ContextBlock`s — `{uri, tier, node_hash,
event_time_ms, text, estimated_tokens, source_ref}` across three tiers (`L0` a short form, `L1` a
compact pointer, `L2` the event text).

**Packing to a budget.** `max_prompt_tokens` (2,048 by default) is the budget. The caller's own
prompt is charged against it first; blocks are then taken **greedily in ranked order**, and a block
that does not fit is recorded in `blocked_blocks` while the fill *continues*, so a small
lower-ranked block can still land after a large one was rejected. Both lists, with the token counts,
are written durably as a `ContextPackAudit` — what went in and what was held back is a record, not
a log line.

**Recency and relevance.** Candidate nodes are scored two ways and merged: a lexical query plan
(topic phrases, stemmed term groups with synonyms, adjacent bigrams, and intent bonuses for
"latest"-shaped and timeline-shaped questions) and **cosine similarity over vectors stored inline on
the node record**, batched 1,000 nodes a command. A lexical match saturates deliberately *below* a
perfect cosine, so in a mixed store a strong semantic match outranks a purely lexical one while an
un-embedded node stays rankable instead of collapsing to recency order. The final block order is
lexical relevance, then tier (`L0` before `L1` before `L2`), then recency, then URI, de-duplicated
by `source_ref`. Freshness is a tiebreak — again, there is no decay curve.

**Keeping a working set resident.** The nodes a caller names are resolved from the resident index
and their blocks from the memory cache tier, on the same path as §4; a cold sweep deliberately does
not fill the cache, so bulk maintenance does not evict a live working set.

**Embedding safety is enforced, not documented.** Vectors of differing width are declined, and
same-width vectors written by a *different* encoder are declined too, counted separately — two
widths means a provider outage seeded fallback vectors, two encoders at one width means a model
swap: same symptom, different cause, different fix. Embeddings are **supplied**, over an
OpenAI-compatible HTTP endpoint; the built-in alternative is a deterministic 16-dimension hash for
tests, and `MATRIXARK_REQUIRE_MODEL_EMBEDDINGS` turns using it into a hard error rather than a
quiet downgrade.

**What it saves, on a recorded run.** On a full local agent corpus of **12,384 events / 97 sessions
/ 1,698,940 tokens**, a token-budgeted pack of **~1,333 tokens** scored **8.30 against 6.59** for
replaying the whole local context, on 6 of 6 queries, with `qwen2.5:7b` as both reader and judge
(2026-08-09). Method, arms and the judge's known failure mode are in
[docs/benchmarks](docs/benchmarks/README.md); that run needs a local model and was **not** re-run
for this document.

**Limits.** There is **no ANN index** — scoring is a linear cosine over the candidate nodes the
caller supplies, and `node_hashes` is mandatory: there is no "search the whole tenant" entry point
in this engine. The secondary-index commands exist and are queryable, but retrieval does not use
them to select candidates; entity records are written and queryable but are not packed. Switching
embedding model means re-embedding the corpus.

## 6. What else the engine actually does

- **Control state** — quotas, pacing, suppression and selection as single-key atomic updates, with
  windowed sums, UUID-keyed idempotency over a 5-minute dedup window, and an optional bounded
  distinct sketch (off by default, so distinct stays exact).
- **A RESP surface** — strings, hashes, sets, sorted sets and lists, plus native verbs for the
  feature, sequence and control-state models. Unsupported commands return a deterministic error
  rather than a fake success, `SELECT` accepts database 0 only, and the TTL semantics of every verb
  are written down. Lists and sorted sets are storage-backed through encoded string values; native
  list and sorted-set models remain future work.
- **Blobs and large content** — a multipart upload (begin / append / commit, content-hashed and
  fsynced before the rename), a single-shot put, **range reads**, and a sweep that collects blobs
  no record points at. Large raw content offloads to content-addressed storage while metadata stays
  in the index.
- **Batch and stream ingestion** — batch requests with per-record results, a dead-letter path with
  an export report, and offset-ledger and checkpoint-handshake surfaces for streaming sources.
- **Namespaces → tables → shards**, with per-table and per-tenant read and write QPS limits,
  per-shard rate and burst limits, a read-only shard mode, and topology-aware placement that spreads
  replicas widest-first across location labels.
- **Crash safety you can check** — the shard index carries a format stamp and **refuses** an image
  older than the one it understands; expiry replay is clock-aware, so a key live at leader time is
  not silently dropped on recovery; fault-injection hooks and a single-barrier crash harness are in
  the tree.
- **Failure detection with guardrails** — an adaptive φ-accrual detector, conviction ratio
  thresholds, a safe mode, and refusals for orphaning shards or self-clearing a conviction.
- **A Rust SDK** with both an in-process client and an HTTP client, and a C ABI for non-Rust
  callers. It lives in `sdk/rust/temporalstore` and is deliberately outside the workspace.

## What this store is bad at

A page that only lists what works is less useful than one that does not.

- **Writes scale sublinearly with concurrency, and the cost comes back as latency.** On the durable
  shared-storage write path, 400 writes a run on one box: **471 writes/s at 2 concurrent writers,
  842 at 4, 1,320 at 8** — four times the writers for **2.80x** the throughput — while write p50
  climbs 3.77 → 4.36 → 4.73 ms and the read p50 behind them climbs 76 → 119 → 247 µs. Concurrency
  does buy something; it does not buy proportionally, and what it costs is the tail. During each
  concurrent burst a replica fell as far as 400 entries behind the primary and converged back to
  **0** after replay. *(`scale_harness --nodes 3 --compare-shared-store true --shared-store-ops 400
  --shared-store-flush-every 25 --shared-store-concurrency N`, release build, one box; all four
  points come from the same arm of the same harness.)* A separate single-box report records a
  throughput plateau beyond eight cores on the HTTP path:
  [docs/ENTERPRISE_INGESTION_RETRIEVAL_PERF.md](docs/ENTERPRISE_INGESTION_RETRIEVAL_PERF.md).
- **Small payloads pay a large fixed envelope.** 36.4 B a record before the payload: 4.71x framed
  over user bytes on an empty value, 1.24x at 128 B, and only at a few kilobytes does it round to
  1.00x. End to end it is worse, because the index log is per record too: total write amplification
  is **4.25x at 64-byte values against 2.06x at 4 KiB**. A workload of tiny writes is paying mostly
  for framing.
- **The cluster figures here are from three nodes in one process on one box.** That is enough to
  measure the consensus/local-read asymmetry and to exercise a failover and two membership changes,
  and it is not a multi-machine result. The multi-process harnesses are in the tree and listed under
  [Build & test](#build--test-from-source); run them on real nodes before sizing a deployment.
- **A per-key figure is a sawtooth.** The table behind a per-key map steps at powers of two, so the
  per-key cost can move 1.99x between two key counts eight apart. Any capacity plan built on a
  single per-key number is built on a phase.
- **Aggregates are linear in the window by default.** The incremental ladder that avoids that is off
  by default and sum-family only, and on the feature path it serves one literal aggregator.
- **No decay, no top-k, no percentiles, no ANN index.** Ranking recency is a tiebreak; retrieval
  scoring is linear in the candidate set the caller names.
- **Retention has no age dimension.** Series retention is by count, TTL is per key, and neither can
  express "drop points older than a week".
- **Authentication is thin.** Two shared-secret environment tokens, no users, no roles, no ACLs and
  no per-key authorization. Consensus transport has TLS; the service edge does not terminate it.
- **Stored formats can still change.** See below.

## Quick start (single node, Docker)

```bash
docker run --rm -p 6379:6379 -p 8080:8080 \
  -v temporalstore-data:/var/lib/temporalstore \
  ghcr.io/matrixarkai/temporalstore:latest
```

Then talk to it with any RESP client:

```bash
# write: key "hello" = bytes for "world"
redis-cli -p 6379 SET hello world

# read it back
redis-cli -p 6379 GET hello
```

The RESP surface, the HTTP routes, and the control verbs are documented under
[`docs/`](docs/). Deploy manuals: [Windows Docker](docs/windows_docker_install.md) ·
[Linux](docs/linux_deploy.md) · [macOS](docs/macos_deploy.md).

## Python tooling and agent integrations

**This repository is the Rust engine only.** The Python context tooling — ingestion, retrieval,
feedback, the agent hooks and the MCP server — lives in
**[matrixarkai/matrixark](https://github.com/matrixarkai/matrixark)**, also Apache-2.0.

Use this repository when you want the engine: embed the crate, run the binaries, or talk to a node
over RESP or HTTP. Use `matrixark` when you want the Python client, the extraction and retrieval
pipeline, or an agent integration.

## Deploy: laptop → replicated cluster

- **Local single node** — one Docker command (above); durable storage in a local volume, no
  metaserver dependency.
- **Distributed** — replicate through **MatrixRaft** consensus; the storage backend auto-resolves
  **object store → shared filesystem → replicated local disk**, and every resolution publishes the
  reason it chose what it chose.
- **Serving modes, budgets, startup and recovery, storage resolution** are documented in
  [docs/benchmarks](docs/benchmarks/README.md) and the deploy manuals above.

## Architecture

```
Clients ── RESP · HTTP · proxy · matrixark (Python)
   │
Engine ─ TemporalStore: temporal index · serving paths · append-structured block store
   │
Foundation ─ MatrixCache (multi-layer cache) · MatrixRaft (Rust Raft consensus)
   │
Storage backend (auto) ─ object store → shared filesystem → local + Raft
```

Five Apache-2.0 repositories, all public and open to anyone in the world to use, fork and
contribute to:
[TemporalStore](https://github.com/matrixarkai/TemporalStore) (engine) ·
[MatrixCache](https://github.com/matrixarkai/MatrixCache) (cache) ·
[MatrixRaft](https://github.com/matrixarkai/MatrixRaft) (consensus) ·
[MatrixObject](https://github.com/matrixarkai/MatrixObject) (object store) ·
[matrixark](https://github.com/matrixarkai/matrixark) (Python tooling).

---

## Build & test (from source)

```bash
cargo check -p temporalstore-rust --all-targets
cargo test  -p temporalstore-rust --lib --tests -- --test-threads=1
```

Focused harnesses:

```bash
cargo run -p temporalstore-rust --bin readiness_gate -- --service-reports
cargo run -p temporalstore-rust --bin context_workflow_harness
cargo run -p temporalstore-rust --bin storage_modes_harness
cargo run -p temporalstore-rust --bin raft_secondary_replication_harness
```

Reproducing the figures on this page: the byte and allocation measurements need the counting
allocator, which is a Cargo feature and test-only, and they are `#[ignore]`d because they seed
stores and weigh them — so they run by name:

```bash
cargo test --release --features alloc-probe -p temporalstore-rust --lib \
  <measurement name> -- --include-ignored --nocapture --test-threads=1
```

Formatting:

```bash
cargo fmt --all -- --check
```

## Before the first milestone: formats can change

Stored formats and wire shapes may still change between releases. The index carries a format stamp
and refuses an image it cannot read rather than guessing, so an upgrade fails loudly instead of
silently mis-reading a store. Pin a version for anything you care about keeping.

## Status & evidence

Apache-2.0. Read the project's production-readiness claims from passing reports rather than from this README:

- [Benchmarks: token & quality against full local replay](docs/benchmarks/README.md)
- [Context management on TemporalStore](docs/context_management_on_temporalstore.md)
- Deploy: [Windows Docker](docs/windows_docker_install.md) · [Linux](docs/linux_deploy.md) ·
  [macOS](docs/macos_deploy.md)

The engine is Rust-native end to end: the temporal index, the serving paths and the block store
are all Rust, with no foreign-language bridge anywhere in a request. Out of scope unless
separately re-added: alternate wire-protocol compatibility (**no brpc/thrift** layer, clients
speak RESP or HTTP), and live external object-store integration.

## Contributing

**Anyone is welcome to contribute, and you do not need to ask first.** If something is wrong,
unclear or missing, open an [issue](https://github.com/matrixarkai/TemporalStore/issues); if you
would rather fix it yourself, send a
[pull request](https://github.com/matrixarkai/TemporalStore/pulls). You need no connection to this
project and no permission: the licence is Apache-2.0 and the fork button works for everyone.

See [CONTRIBUTING.md](docs/CONTRIBUTING.md) and [SECURITY.md](docs/SECURITY.md). Contributions
target `main`, this repository's default branch and the branch every change merges to. The
older `rust-main` name is retired and no branch by that name exists here, so a pull request
opened against it has nowhere to land.

## License

Licensed under the Apache License, Version 2.0 ([`LICENSE`](LICENSE), [`NOTICE`](NOTICE)).
Third-party dependency licenses and attributions are listed in
[`THIRD-PARTY-NOTICES.md`](docs/THIRD-PARTY-NOTICES.md). Product and crate names are trademarks
of MatrixArkAI; see [`TRADEMARKS.md`](docs/TRADEMARKS.md).
