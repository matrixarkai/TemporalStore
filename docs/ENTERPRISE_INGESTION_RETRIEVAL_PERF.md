# Enterprise Ingestion & Retrieval Performance Validation

API-driven ingestion and retrieval of TemporalStore at high QPS and with large files,
on the enterprise setup **TemporalStore + MatrixStore (MatrixObject / shared storage for
large attachments) + MatrixKV** (metadata-as-KV-in-TemporalStore is the default; MatrixKV
optional). This report measures latency, max QPS and concurrent ingest+retrieval QPS under
fixed CPU counts, drives several optimization iterations, and records the recommended
production configuration.

Author: bjmeetsfo. Worktree `/root/ts-perf` off `origin/main`. Release builds, single
machine (WSL Ubuntu 22.04, 16 physical cores). CPU counts enforced with
`taskset -c 0-(N-1)`. Because this is a single box, read the **deltas and the
bottleneck→fix story**, not the absolute QPS ceiling.

## Executive summary

The binding bottleneck for API ingestion was **per-write WAL fsync durability**, not the
network or the engine. Enabling group-commit / relaxed-fsync ingestion
(`MATRIXARK_BULK_INGEST=1`) plus datanode/shard fan-out lifts sustained ingest throughput
by **~40-70x** with zero errors.

| Metric (recommended config: 8 CPU, bulk-ingest, 16 workers, 8 datanodes, 16 shards) | Result |
|---|---|
| **Ingest (write-only) QPS** | **~4,700 ops/s**, write p50/p95/p99 = 2.7 / 7.8 / 11.1 ms |
| **Retrieval (read) latency / QPS** | read p50/p99 = 1.5 / 8.5 ms; read-heavy mix **~6,000 ops/s** |
| **Concurrent ingest+retrieval QPS** | **~5,200 ops/s** (mixed r+w+batch), W p50 2.6 / R p50 1.7 ms, 0 errors |
| **HTTP keep-alive uplift (Iter 4)** | **+47-52% mixed QPS**, read p50 −60..−73% (interleaved A/B, both core counts) |
| **Large-file throughput (blob-store layer)** | 1 MB w164 / r881 · 16 MB w412 / r436 · 128 MB **w411 / r419 MB/s** |
| **Large-file throughput (e2e via HTTP, Iter 5)** | 1 MB w148 / r409 · 16 MB w178 / r132 · 128 MB **w103 / r114 MB/s** |
| **Shared-store durable storage write** | sync p50/p99 = 3.8 / 8.8 ms; async enqueue ≈ 0 ms (deferred, group-flushed) |
| **Error budget at recommended config** | 0 failed ops |

Baseline (durable per-write fsync, same box) peaked at only **~68-120 ops/s** for the API
KV path and collapsed into timeouts beyond ~16-32 concurrent writers. Single-writer
isolation makes the cause unambiguous: **write p50 19.5 ms → 0.84 ms (23x)** once per-write
fsync is deferred, while reads were already ~0.9 ms in both modes. The KV QPS figures above are
from the first measurement session (cleaner host state); the follow-up keep-alive gain is quoted
as an interleaved A/B delta because absolute single-box QPS drifts between sessions with host
thermal/IO load (see Iteration 4).

## Test surface

- **Concurrent API QPS/latency** — `crates/temporalstore-rust/src/bin/client_scale_harness.rs`:
  real datanode HTTP servers + metaserver + a real routing client, exercising the KV
  `/execute` + `/batch_execute` path (real HTTP + engine + WAL). Reports write/read/batch
  p50/p95/p99, ops_per_sec, route-cache stats, errors.
- **Shared-store (MatrixObject / FileObjectStore) sync-vs-async** —
  `src/bin/scale_harness.rs --compare-shared-store`.
- **Large-file / attachment throughput** — `examples/large_blob_throughput_bench.rs` (added
  in this work) driving the `ObjectStore::append_blob` chunked path directly, since the HTTP
  layer does not yet stream blobs end-to-end.
- **Isolation probe** — a standalone datanode (`TS_STANDALONE=1`) hit directly with `curl`
  to measure the raw HTTP+engine floor.

The `matrixobject` feature is a git dependency (`MatrixObjectStore`); with the proxy down it
was not fetched, so the shared-storage tier was measured against the in-tree
`FileObjectStore` (fsync-per-put) — a conservative stand-in for the MatrixObject blob store.

## How the bottleneck was found

The API KV path showed ~40-67 ms per-op latency and ~120 ops/s peak — suspiciously close to
the classic TCP delayed-ACK timer. Two probes decomposed it:

1. **Raw datanode probe**: `POST /execute` against a standalone datanode returned **p50
   0.81 ms** (new connection per request). The HTTP + engine path is *not* the problem.
2. **Single-writer harness**: with zero contention, **reads = 0.93 ms but writes =
   19.5 ms**, and the client issued exactly one POST per write (meta_sync_total = 0). The
   ~19 ms is entirely **server-side durable write handling**, i.e. per-write WAL fsync
   (`wal.rs`: `append_with_sync(..., !wal_bulk_relaxed_durability())`).

TCP_NODELAY / Nagle was a secondary factor (fixed anyway — see Iteration 1) but not the
dominant cost; fsync was.

## Per-iteration results

All API rows: `client_scale_harness`, mixed unless noted, `taskset` CPU pinning, medians of
2-3 reps. "Workers" = clients × threads-per-client. Each harness process uses a fresh
per-process temp store (`engine::unique_temp_path`), so runs do not share on-disk state.

| Iter | Setup (CPU · datanodes · shards · workers · durability) | Ingest QPS | Concurrent QPS | Write p50/95/99 | Read p50/95/99 | Errors | Bottleneck → fix |
|---|---|---|---|---|---|---|---|
| **0 baseline** | 8 · 4 · 8 · 8 · **durable fsync** | ~68-120 | ~68-120 | 90 / 289 / 630 ms | 50 / 212 / 363 ms | 44 (timeouts) | Per-write WAL fsync; collapses under concurrency vs 500 ms io-timeout |
| 0 (1 writer) | 8 · 4 · 8 · **1** · durable | 64 | — | 19.5 / 35 / 50 ms | 0.9 / — / 2.3 ms | 0 | Isolates fsync as the cost (reads unaffected) |
| **1** | 8 · 4 · 8 · 8 · durable + **TCP_NODELAY + write coalescing** (code) | ~113-120 | — | 58 / 182 / 360 ms | 32 / 139 / 257 ms | ~8 | Nagle/delayed-ACK removed; tail latency + robustness improve, but fsync still dominates |
| **2** | 8 · 4 · 8 · 8 · **`MATRIXARK_BULK_INGEST=1`** (relaxed/group-commit fsync) | 1,550 | 1,550 | 4.1 / 12.9 / 20.2 ms | 2.3 / 10.8 / 16.7 ms | 0 | fsync deferred → **22.8x** vs baseline; single-writer 19.5 → 0.84 ms |
| **3** | 8 · **8** · **16** · **16** · bulk-ingest (datanode/shard fan-out + more clients) | **4,708** | **5,176** | 2.6 / 7.8 / 11.1 ms | 1.7 / 6.7 / 10.2 ms | 0 | Spreads single-shared-log lock + per-client stats lock → **~1.4x** over iter 2 config, ~40-70x over baseline |

Notes:
- Iteration 2's clean, isolated peak (small burst) reached **~2,300-4,200 ops/s**; per-write
  cost grows with store size (the O(store) served-index maintenance), so sustained
  throughput settles lower than an initial burst. Iteration 3 numbers are steady-state
  medians at a realistic store size.
- Read latency was already ~0.9 ms and did not change across iterations — **retrieval was
  never the bottleneck**; ingestion durability was.

### CPU scaling of the recommended (bulk-ingest) config

Max sustained mixed QPS, tuning workers/datanodes per core count:

| CPUs | best workers · datanodes | Concurrent QPS (median) | Write p50/95/99 | Read p50/95/99 | Scaling |
|---|---|---|---|---|---|
| 4 | 6 · 4 | 3,994 | 1.3 / 3.4 / 4.8 ms | 0.8 / 2.7 / 4.1 ms | 1.0x |
| 8 | 16 · 8 | 5,176 | 2.6 / 7.7 / 11.1 ms | 1.7 / 6.7 / 10.2 ms | 1.30x |
| 16 | 24 · 8 | 5,287 | 3.3 / 12.1 / 18.9 ms | 2.4 / 11.2 / 16.9 ms | 1.32x |

Throughput scales ~1.3x from 4→8 cores then **plateaus ~5,300 ops/s** at 16 cores. The
ceiling is a single-box structural limit, not durability: (a) the HTTP layer uses a new TCP
connection per request (`Connection: close`) with a thread spawned per connection, and the
per-op engine work (bulk write + served-index maintenance, which grows with store size).
Iteration 4 (keep-alive) addressed the connection-churn half. The per-client `Mutex<ClientStats>`
was **tested and ruled out** as a ceiling: at 16 workers / 8 CPU, one client shared by 16 threads
(max stats-lock contention) sustained ~4,280 ops/s vs ~4,000 ops/s for 16 independent
single-thread clients (zero contention) — i.e. no penalty, because the critical section is a
single field increment dwarfed by the ~2-3 ms/op work. So an atomic-counter refactor was **not**
pursued. The remaining structural lever is the **delta/incremental served-index** to flatten the
store-size write curve (large, separate change).

### Shared-store storage tier (sync vs async durability)

`scale_harness --compare-shared-store`, FileObjectStore (fsync-per-put), 8 CPU:

| Path | Write p50/p95/p99 | Read p50 | Concurrent (c=8) |
|---|---|---|---|
| Sync durable storage write | 3.8 / 6.6 / 8.8 ms | 0.98 ms | write p50 105 / p99 283 ms · ~40 write qps; read p50 3.5 ms · **1,188 read qps** |
| Async storage enqueue | ≈ 0.00 ms (buffered) | 0.91 ms | flush batches ~100 ms per 25 writes (amortized) |

The async storage writer makes the primary write path non-blocking (enqueue ≈ 0 ms) and
group-flushes to the shared object store; sync durable writes cost one fsync (~4 ms). The
concurrent-sync write path is bound by the single shared-log lock (~40 write qps), which is
exactly why **datanode/shard fan-out** is the throughput lever at the API layer.

### Large-file / attachment throughput

`large_blob_throughput_bench`, FileObjectStore blob path (chunked `append_blob`, 1 MB
chunks), 8 CPU. The HTTP layer does not stream blobs yet, so this is measured at the
blob-store layer directly.

| Attachment size | Write MB/s | Read MB/s | Write ms/1MB-chunk |
|---|---|---|---|
| 1 MB | 164 | 881 | 6.1 |
| 16 MB | 412 | 436 | 2.4 |
| 128 MB | **411** | **419** | 2.4 |

Steady large-file throughput ~410 MB/s write / ~420 MB/s read (fsync-per-chunk file store,
sustained and flat from 16 MB to 128 MB). A real MatrixObject store with extent batching is
expected to meet or exceed this.

## Recommended production configuration

For ingest-heavy enterprise workloads (metadata KV + large attachments):

- **Durability**: `MATRIXARK_BULK_INGEST=1` (relaxed/group-commit WAL fsync) on the ingest
  datanodes. This is the single highest-leverage setting (~20-60x). Durability is preserved
  at group-commit granularity; pair with replication (shared-store/raft) for the crash
  window. Keep strict per-write fsync only where per-record durability is contractually
  required (accept ~100-200 ops/s/writer there).
- **Fan-out**: several datanodes and shards (≈ 2x the writer concurrency in shards; ≥ 8
  datanodes at 8+ cores) to spread the single shared-log lock. This is what turns cores into
  QPS.
- **Storage backend**: async storage (proxy default `MATRIXARK_RUST_PROXY_ASYNC_STORAGE`
  = true) so the primary write path is non-blocking; large attachments to the MatrixObject
  blob store via the `append_blob` extent path.
- **Concurrency**: ~1.5-2 writer threads per core, spread across multiple client instances
  (independent lock domains). Cache: the page-cache default is now
  derived from system memory (a sixteenth per engine, 128 MiB floor, 512 MiB ceiling,
  bounded across a process at a quarter of memory) rather than a fixed 128 MiB. A cache
  smaller than the working set is what makes writes look linear in corpus size: on one
  260 MB store the same ingest took 3 291 ms at 128 MiB, 1 157 ms at 384 MiB and 902 ms at
  1 GiB. `MATRIXARK_RUST_PROXY_CACHE_BYTES` still overrides it per engine.
- **Sizing**: expect ~5,000 concurrent ops/s and ~4,700 ingest ops/s per 8-core box at these
  latencies; scale out horizontally beyond that (single-box throughput plateaus ~5,300 ops/s
  by 16 cores until the connection-per-request and per-client-lock limits are addressed).

## Follow-up optimization pass

### Iteration 4 — HTTP/1.1 keep-alive + client connection pooling

**Change (`src/http.rs`):** the datanode server now serves every request on a connection in a
keep-alive loop (`Connection: keep-alive`, honoring `Connection: close`, with an idle-reap
read timeout) instead of one-request-then-close; the client keeps a **thread-local pool of
idle sockets per destination** and reuses them, falling back to a fresh connection (and
retrying transparently) when a pooled socket has been reaped. This removes the per-request TCP
handshake **and** the per-request server thread spawn that were the churn source behind the
~5.3k single-box plateau.

**Measurement note (important):** absolute QPS on this single box drifts with host thermal / IO
state between sessions (the same binary measured ~5.3k in the baseline session and ~2.5-3.8k
here after many back-to-back builds). To remove that drift the before→after was taken as an
**interleaved A/B**: OLD (commit 29250fdd, connection-per-request) and NEW (keep-alive) binaries
run alternately in the same session, 4-5 reps each, medians reported. Read the **deltas**.

| Config | OLD ops/s (med) | NEW ops/s (med) | Δ throughput | OLD read p50 | NEW read p50 | OLD write p50 | NEW write p50 |
|---|---|---|---|---|---|---|---|
| 8 CPU · 16 workers · 8 datanodes | 2,560 | **3,756** | **+47%** | 2.71 ms | **0.74 ms** (−73%) | 4.76 ms | 2.92 ms |
| 16 CPU · 24 workers · 8 datanodes | 1,993 | **3,035** | **+52%** | 4.78 ms | **1.97 ms** (−59%) | 8.20 ms | 5.49 ms |

Keep-alive delivers **+47-52% mixed throughput** and **−60 to −73% read latency** at both core
counts, with zero backend errors (the pooled-socket reuse was confirmed: `backend_errors = 0`,
route-cache hit-rate unchanged). Reads benefit most because they no longer pay a handshake per
lookup. A unit test (`keep_alive_reuses_a_single_connection_for_many_requests`) pins the
behavior: a server that accepts exactly one connection serves five sequential requests over it.

### Iteration 5 — large-file streaming wired end-to-end through HTTP

**Change (`src/bin/server.rs`, new `src/bin/blob_http_bench.rs`, `src/http.rs`):** the datanode
now exposes `POST|PUT /blob/<key>` and `GET /blob/<key>`. The upload chunks the request body
straight into `ObjectStore::append_blob` (the same primitive `shared_store`'s
`append_blob_with_retry` calls) in `TS_BLOB_CHUNK_BYTES` slices (default 1 MiB), so a single
attachment does not force a monolithic write; the download streams the stored object back. A
shared multi-thread tokio runtime bridges the sync HTTP handler to the async object store. This
**closes the earlier "large-file HTTP streaming not wired e2e" caveat** — attachments now flow
through the real datanode HTTP body, not just the blob-store layer. Backend: `FileObjectStore`
(a MatrixObject store drops in behind the same `ObjectStore` trait once the feature builds).

End-to-end HTTP throughput (standalone datanode, 8 CPU, 1 MiB server-side chunking, best of 3):

| Attachment | HTTP write MB/s | HTTP read MB/s | write ms | read ms | vs raw blob-store (write/read) |
|---|---|---|---|---|---|
| 1 MB | 148 | 409 | 6.7 | 2.4 | 164 / 881 |
| 16 MB | 178 | 132 | 89.7 | 121.2 | 412 / 436 |
| 128 MB | **103** | **114** | 1237 | 1125 | 411 / 419 |

Correctness verified: every GET returns exactly the uploaded byte length. The HTTP path runs
~2.5-4x slower than the raw blob-store layer at 128 MB because the current generic HTTP handler
**fully buffers** the body and makes several full-size copies (client frames header+body into one
buffer; server accumulates the request, then builds one header+body response buffer; each chunk
is `copy_from_slice`d into the store). True zero-copy streaming (socket → store without full
buffering) needs the generic `serve` handler signature to expose the socket to a streaming sink;
that refactor is the documented next step. Even buffered, the e2e path sustains ~100 MB/s per
attachment — adequate for the attachment sizes this tier targets.

### Iteration 6 — blob zero-copy: streaming HTTP `/blob` (no full-body buffering)

**Change (`src/http.rs`, `src/bin/server.rs`, `crates/temporalstore-snapshot/src/object_store.rs`):**
this delivers the "documented next step" from Iteration 5. The generic HTTP server grew a
**streaming path** alongside the buffered one:

- `serve_with_stream_handler(addr, stream_handler, handler)` — the server reads only the request
  **head** off the socket, then offers each request to a `stream_handler` with the socket exposed
  via a new `StreamTransfer`. The handler `read_body()`s the request body in caller-sized chunks
  straight off the socket and `send_head()` / `write_chunk()`s the response straight back — the
  body is **never** assembled into a `Vec`. Returning `StreamAction::Declined` (without touching the
  body) falls through to the unchanged buffered `handler`, so every non-`/blob` route and the
  small-request fast path behave exactly as before. Plain `serve()` now delegates with a
  declines-everything stream handler. The keep-alive loop and `Connection: close` handling are
  intact (keep-alive is parsed from the head; the streamed response emits the matching header).
- The datanode's `/blob/<key>` handler is now the stream handler: **POST/PUT** loops
  `socket → append_blob` in `TS_BLOB_CHUNK_BYTES` (default 1 MiB) slices — memory bounded to one
  chunk, never the whole upload; **GET** `stat`s the object (`FileObjectStore::object_path`, new),
  sends `Content-Length`, then copies `file → socket` in chunks. This removes the three full-size
  copies Iteration 5 called out (server request accumulation, `Bytes→Vec` on read, and the
  coalesced header+body response buffer).

End-to-end HTTP throughput, **release** build, standalone datanode, 1 MiB chunk, best of 3,
**interleaved A/B in one session** (OLD = buffered `86a96726`, NEW = streaming), read the deltas:

| Attachment | OLD w / r MB/s | NEW w / r MB/s | Δ write | Δ read |
|---|---|---|---|---|
| 16 MB  | 190 / 135 | **281 / 767** | +48% | **+5.7×** |
| 64 MB  | 119 / 103 | **189 / 241** | +58% | +2.3× |
| 128 MB | 113 / 104 | **188 / 343** | +66% | +3.3× |
| 256 MB | 114 / 106 | **167 / 246** | +47% | +2.3× |

(The OLD column reproduces Iteration 5's buffered figures — 128 MB w113/r104 here vs the earlier
w103/r114 — confirming the A/B baseline.) Writes gain **+47–66%**; reads gain **2.3–5.7×** because
the download no longer loads the whole object into memory and makes zero response-side copies. The
write side is now bounded by `FileObjectStore`'s `append_blob` fsync-per-chunk rather than by body
buffering; at 128 MB the streamed write (188 MB/s) closes roughly half the remaining gap to the raw
blob-store ceiling (411 MB/s), and the streamed read (343 MB/s) reaches ~80% of it. Correctness
verified: every GET returns exactly the uploaded byte length across all sizes.

### Iteration 7 — delta / incremental served-index — **DEFERRED (design recorded)**

**Goal:** flatten the steady-state ingest write curve. On the **synchronous** durability path
(`config.async_storage == false`, non-bulk), every write re-serializes the whole shard index and
rewrites it durably: `engine.rs` `execute` → `serialize_index(shard)` (O(store) CPU) →
`index_log_store.append_json(whole index)` (O(store) IO) → `persist_index_bytes` →
`atomic_write_bytes` (temp write + fsync + rename of the whole `shard-{id}.index.json`, O(store) IO).
So per-write cost grows with store size → O(n²) over an ingest, and steady-state QPS falls below the
cold-burst rate. (The **async** path — the validated default, used by the live hook — already skips
this per-write persist and only appends to the WAL, so it is unaffected.)

**Why deferred (not landed this pass).** A correct fix must be *genuinely incremental* — write only
the delta and reconstruct on read — **not** a persistence-timing deferral. The served index file is
a load-bearing **complete-`ShardState`** contract read by 8+ independent consumers that each trust
it as current and do **not** replay the WAL to catch up: restart recovery (`load_index`,
`persistence.rs`), dump-manifest creation (`export_index_bytes` → sha256 + `ShardState` deserialize,
`bucket_dump_manifest_methods.rs`), shared-store index upload (`shared_store.rs`), `StreamKind::Index`
reads (`stream_batch_methods.rs`), recovery/compaction (`recovery_sweep_compact.rs`), plus engine
tests that read the raw file bytes and assert exact content/sha (`engine/tests/part3.rs`,
`part4.rs`). A prior persistence-timing deferral broke **55 storage tests** for exactly this reason
(recorded in project history). Making the write incremental therefore requires re-routing *every*
reader through a base+delta reconstruction — a broad, durability-sensitive change touching recovery,
dump, and shared-store paths that cannot land safely in the same pass as Iteration 6 on a highly
concurrent `main`. Per the iteration's own risk guidance, Iteration 6 ships alone rather than risk a
regression here.

**Concrete design for the separate pass.** Split the served index into an immutable **base snapshot**
plus an append-only **delta log**:

1. `shard-{id}.index.json` stays the base snapshot but is rewritten only on compaction events
   (flush / dump / gc / unload), not per write.
2. Add `shard-{id}.index.delta.jsonl`: each sync publish appends **only the changed records/keys**
   (keyed delta entries + the `applied_wal_sequence` anchor) — O(delta) fsync, not O(store).
3. Introduce one `load_served_index_bytes(shard_id)` helper that reads the base and folds the delta
   log on top into a complete `ShardState`, and **route all 8 readers** listed above through it
   (`export_index_bytes` returns the reconstructed bytes so dump/shared-store/stream stay byte-exact;
   the sha256 is computed over the reconstruction). `load_index` reconstructs on restart.
4. Compact the delta log back into the base when it exceeds a size/entry threshold, and always at
   flush/dump/gc (which already rewrite the whole index), so the delta never grows unbounded and the
   existing durable-anchor / manifest-generation invariants are preserved.
5. Update the raw-bytes tests to assert over the reconstruction helper rather than a single file.

This keeps every consumer seeing a correct, current index while making the per-write publish
O(delta). Steady-state small-store-vs-large-store curve numbers belong to that pass (this pass
records no fabricated figures for a change it did not land).

## Correctness & safety

- Iteration 0-3 code change (`src/http.rs`): `TCP_NODELAY` on accepted server + client sockets,
  header+body coalesced into a single write. Iteration 4 (`src/http.rs`): HTTP keep-alive server
  loop + thread-local client connection pool. Iteration 5 (`src/bin/server.rs`, `src/http.rs`):
  streamed `/blob/<key>` attachment endpoints + `request_bytes_with_options` raw-body client.
- Single-threaded lib suite **605 passed / 1 failed** — the one failure
  (`data_node … storage_manager … jitter_backoff`) is the pre-existing environmental flake also
  present on the `main` baseline (604/1; 605 here because of the added keep-alive test).
  **Zero regressions.**
- New files: `examples/large_blob_throughput_bench.rs` (blob-store-layer bench) and
  `src/bin/blob_http_bench.rs` (end-to-end HTTP attachment bench).

## Honest caveats

- **Single-box bounds absolute QPS.** All servers, clients and meta run in one process on one
  machine; absolute numbers are a floor for a real multi-node deployment and a ceiling for
  what one box can do. Read the deltas.
- **Large-file HTTP path buffers (no zero-copy yet).** As of Iteration 5 attachments flow
  end-to-end through the datanode HTTP body (`POST|GET /blob/<key>` → `ObjectStore::append_blob`),
  but the generic HTTP handler still fully buffers the body and makes a few full-size copies, so
  the HTTP path runs ~2.5-4x below the raw blob-store layer at 128 MB. True socket→store
  zero-copy streaming needs the `serve` handler signature refactored to expose the stream.
- **`matrixobject` feature not built** (git dep unreachable behind the proxy); the shared /
  large-attachment tier used `FileObjectStore` as a conservative fsync-per-op stand-in.
- **Bulk-ingest is a durability trade-off** (group-commit vs per-write fsync); recommended
  for ingest throughput with replication, not for per-record synchronous-durability SLAs.
- **Store-size sensitivity**: per-write cost grows with store size (served-index
  maintenance), so a cold burst benchmarks higher than sustained ingestion; the reported
  QPS are steady-state medians.
- The `client_scale_harness` spawns a thread per HTTP connection and cannot reliably bind
  >~128 in-process servers/clients (port/thread pressure), which caps how far concurrency can
  be pushed *in this harness* — not a property of the datanode itself.
