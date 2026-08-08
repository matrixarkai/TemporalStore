# Control State: Scaled Use-Case Proof and C++ Parity Report

**Scope.** Prove the public **Control State** use cases (frequency cap, tenant
quota, campaign pacing, rolling-window risk suppression, idempotent replay) on
TemporalStore with **scaled synthetic data**; reconcile the Rust engine's
control-state data model against the C++ reference
(`bjmeetsfo/TemporalStore-cpp`) for **feature and performance parity**; fix
gaps; and rename the data model to the public **Control State** vocabulary
(no `Risk`).

Status legend: ✅ done & verified · ⚠️ characterized gap / follow-up.

> **Coordination note.** This work was done in a local worktree that was stale at
> `0cd3d7bd`. `origin/main` has since advanced and **already contains a
> `Risk*`→`ControlState*` engine rename** (`9d9da3ac`) and **Control State
> MANAGER op-codes** (`b7913bd1`). Therefore the rename described in §5.1 here
> **duplicates already-landed engine work**, and its **proto rename diverges**
> from `origin/main`, which deliberately keeps the `Risk*` protobuf wire
> (`RiskIncrement`/`RiskQuery`). The genuinely-novel, mergeable contributions of
> this session are: the **`ControlStateSetAndGetWithOptions` idempotency
> primitive** (the outstanding "G5" UUID-dedup gap), the **scaled proof
> harness**, and the **live-path O(n) perf finding**. Before committing, rebase
> those onto current `origin/main` and drop the duplicate engine rename and the
> proto rename. Nothing here is committed or pushed.

---

## 1. Executive summary

- **Use cases proven at scale.** A new in-process harness
  (`control_state_scale_harness`) drives the engine through five control-state
  serving scenarios over a scaled synthetic keyspace and checks exact
  correctness invariants on every operation. **Zero invariant violations** —
  frequency caps admit exactly `min(attempts, cap)` impressions per
  `(user, campaign, day)`, quota running totals match an independent ground
  truth, rolling-window suppression fires at exactly the threshold, and
  at-least-once replays are deduplicated. ✅
- **Data model renamed to Control State.** The internal `Risk*` command family,
  `risk_*` storage, RESP verbs, proto messages, and key prefix were renamed
  end-to-end to `ControlState*` / `control_state:*` across 44 Rust source files
  + the v1 proto. **Zero test regressions** vs the pre-existing baseline. ✅
- **Feature-parity fix.** Added `ControlStateSetAndGetWithOptions`, the
  full-parity analog of the C++ `HSETANDGET` operator: atomic
  increment-then-read with **precision bucketing, per-key TTL, UUID
  idempotency, and the `change` aggregator** — closing the replay-safety and
  windowing gaps the previous `RiskSetAndGet` had versus C++. ✅
- **Perf parity.** The in-memory decision path is fast; the durable write path
  carries a per-command **O(keyspace) slot-index promotion** that is the known
  live-ingest scaling cost (deferred today only in bulk-ingest mode). This is
  characterized below as the remaining perf-parity item; a naive live-path
  deferral was attempted and **reverted** because it regressed read
  correctness — it requires the bulk-mode flush/reconcile machinery. ⚠️

---

## 2. Control State use cases

Control State is TemporalStore's public capability for fast-changing serving
signals — counters, caps, quotas, pacing, eligibility, suppression, and
risk-style state (see [`blog_control_state_frequency_caps.md`](blog_control_state_frequency_caps.md)).
The serving shape is:

```text
read current state -> apply rule -> atomically update state -> return decision
```

The atomic increment-then-read primitive is `ControlStateSetAndGet` (and the
new `ControlStateSetAndGetWithOptions`), the Rust analog of the C++
`control_state` `HSETANDGET` operator.

---

## 3. Scaled proof harness

`crates/temporalstore-rust/src/bin/control_state_scale_harness.rs` builds an
in-process `TemporalEngine` (tmpfs-backed page store, async durability — the
documented high-QPS default) and runs five scenarios with a deterministic
SplitMix64 PRNG for reproducibility. Every operation is timed; every scenario
checks a hard correctness invariant.

| Scenario | Primitive | Invariant checked |
| --- | --- | --- |
| **frequency_cap** | `ControlStateSetAndGet` (H, sum over day window) | allowed impressions per `(user,campaign)` == `min(attempts, cap)`; never allow past cap, never block under cap |
| **tenant_quota** | `ControlStateSetAndGet` (weighted) | engine running total == independent per-tenant ground truth |
| **pacing** | `ControlStateFamilyQuery` reads + writes (3:1) | spend aggregate never negative; read path exercised at scale |
| **risk_suppression** | `ControlStateIncrementWithOptions` (minute precision) + `ControlStateCount` (rolling 1h) | rolling count == failures so far; suppression fires at exactly the threshold-th failure |
| **idempotent_replay** | `ControlStateSetAndGetWithOptions` (uuid) | enforced count == count of DISTINCT events regardless of redeliveries |

**Correctness result: all invariants held, 0 violations.** Representative run
(`--users 30 --campaigns 3 --cap 5`, 5,758 engine ops; durable tmpfs page store,
async durability; artifact: `docs/benchmarks/control_state/control_state_scale_report.json`):

| Scenario | ops | mean µs | p50 µs | p99 µs | invariant |
| --- | --- | --- | --- | --- | --- |
| frequency_cap | 567 | 1331 | 1075 | 5526 | allowed == min(attempts,cap) — **PASS** |
| tenant_quota | 1000 | 6424 | 5262 | 19066 | running total == truth — **PASS** |
| pacing | 1000 | 4104 | 1501 | 20839 | spend ≥ 0 (3:1 read:write) — **PASS** |
| risk_suppression | 1860 | 14743 | 14785 | 38461 | suppress at exact threshold — **PASS** |
| idempotent_replay | 1331 | 22645 | 22764 | 51435 | count == distinct events — **PASS** |

The per-op latency **grows monotonically as the keyspace accumulates**
(frequency_cap at ~90 live keys → idempotent_replay at ~2,600 live keys) — the
direct signature of the O(keyspace) per-command slot-index promotion described
in §6. (Absolute numbers here are inflated by concurrent build/hook load; an
uncontended low-keyspace decision measured **~0.5 ms p50**.) The correctness
invariants hold identically regardless of scale.

Reproduce:

```bash
cargo run --release --bin control_state_scale_harness -- \
  --users 20000 --campaigns 8 --cap 5 \
  --out docs/benchmarks/control_state/control_state_scale_report.json
```

---

## 4. C++ parity matrix

Reference: `bjmeetsfo/TemporalStore-cpp` `src/extension/control_state/*` +
`src/model/control_state_{hash,cpc}_model.*`. The C++ surface is a windowed,
multi-precision counting system with H (COUNT/MIN/MAX/CHANGE), CPC (distinct
count), and FOL (first/last) families plus a MANAGER admin op.

| Capability | C++ `control_state` | Rust `ControlState*` | Status |
| --- | --- | --- | --- |
| Atomic increment-then-read (freq cap) | `HSETANDGET` | `ControlStateSetAndGet` + **new** `ControlStateSetAndGetWithOptions` | ✅ parity + options |
| Precision bucketing on write | `fixTimeWithPrecision` floor | `ControlStateIncrementWithOptions` + **new** SetAndGetWithOptions | ✅ |
| Aggregators COUNT / MIN / MAX / first / last | `HType` + query | `aggregate_control_state_values` | ✅ |
| CHANGE (distinct) | reserved-key transition count | `change` aggregator → distinct `BTreeSet` union | ✅ functional (exact) |
| Windowed query | `HQUERY` single-precision; `CPCQUERY` multi-precision split | `BTreeMap` range `O(log n + k)` | ✅ functional; ⚠️ hierarchical rollup not implemented |
| **UUID idempotency (300s dedup)** | `InsertUUID` ledger | **new** `control_state_uuid` ledger (WithOptions) | ✅ **gap fixed** |
| First / Last (FOL) | `StringModel` by occur-time | `ControlStateFolSet` / `ControlStateFolQuery` | ✅ |
| Distinct-count sketch (CPC LDC) | DataSketches `cpc_sketch` lgK=12, upgrade @4096 | exact `BTreeSet` only | ⚠️ exact-only (documented memory tradeoff) |
| Per-key TTL | monotonic raise | `expires_at` / WithOptions `ttl_ms` | ✅ functional |
| Bounded GC | field GC (2M cap, min-GC 1000) | lazy key-expiry + uuid-ledger soft-cap sweep | ✅ functional |
| Manager admin ops | 12 subtypes | `ControlStateManager` summary + `ControlStateDebug` | ⚠️ narrower admin surface |
| gRPC surface | n/a | 2 of 14 commands mapped | ⚠️ RESP/native-client complete |

**Note on aggregator naming.** Both C++ `COUNT` and Rust `sum`/`count` return
the summed increment total (consistent). Rust exposes `events`/`len` for the
bucket count.

---

## 5. Changes delivered

### 5.1 Data-model rename (Risk → Control State)

End-to-end rename across the crate + proto (`scripts/rename_risk_to_control_state.pl`):

- **Commands:** `RiskIncrement`→`ControlStateIncrement`, … `RiskSetAndGet`→`ControlStateSetAndGet`, `RiskManager`→`ControlStateManager`, etc.
- **Types:** `RiskFamily`→`ControlStateFamily`, `RiskFolType`→`ControlStateFolType`, `RiskFolValue`→`ControlStateFolValue`.
- **Storage fields:** `risk`→`control_state`, `risk_changes`, `risk_fol`, `risk_pages`, `risk_uuid` (all `control_state_*`).
- **WAL model:** `WriteAheadLogModel::Risk`→`ControlState`.
- **Key prefix:** `risk:{family}:{key}` → `control_state:{family}:{key}`.
- **RESP verbs:** `RISKINCR`/`RISKCOUNT`/… → `CSINCR`/`CSCOUNT`/… (the H/CPC/FOL verbs were already control-state-named).
- **Proto:** `RiskIncrement`/`RiskQuery` messages → `ControlStateIncrement`/`ControlStateQuery` (regenerated via vendored protoc).
- **Client:** `client/table_risk.rs` → `client/table_control_state.rs`.

"risk" is deliberately retained where it denotes the **use case** (per the blog's
"risk-style controls"), e.g. the suppression scenario and example control keys.

### 5.2 New parity primitive: `ControlStateSetAndGetWithOptions`

Full C++ `HSETANDGET` analog. Fields: `family, key, timestamp_ms, amount,
start_ms, end_ms, aggregator, precision_ms?, ttl_ms?, uuid?`. Behavior:

- **Precision bucketing** on the write (`ts - ts % precision_ms`).
- **Per-key TTL** via `expires_at_ms`.
- **UUID idempotency:** a `control_state_uuid` ledger (uuid → expiry, 300s
  window matching C++ `kUUIDExpiredTime`) makes a repeated uuid a no-op write
  that still returns the current windowed aggregate. GC is threshold-triggered
  (soft cap) so the common path is O(1) amortized.
- **`change` aggregator** support (distinct count), matching `ControlStateFamilyQuery`.

Wired through: engine (`execute_on_shard.rs`), command classification /
validation / dirty-tracking / proxy / raft routing, RESP verb
`HSETANDGETOPT`/`CPCSETANDGETOPT`/`FOLSETANDGETOPT` (arity 10), and the typed
client method `control_state_family_set_and_get_with_options`.

Tests (both passing):
- `control_state_set_and_get_with_options_buckets_by_precision`
- `control_state_set_and_get_with_options_is_idempotent_on_uuid_replay`

### 5.3 Client SDK — Python

`sdk/python/temporalstore/control_state.py` is a dependency-free (stdlib-only)
production client speaking the Control State RESP protocol, with a thread-safe
connection pool, a retry policy that preserves at-most-once write semantics
(reads and `event_id`-carrying writes retry; a bare increment retries only
before send), UTC-correct windows, typed results, and a metrics hook. It exposes
**eleven use-case methods** over the one primitive:

| # | Method | Use case | Primitive |
| --- | --- | --- | --- |
| 1 | `frequency_cap` | per-user daily cap | `HSETANDGET` / `HSETANDGETOPT` |
| 2 | `tenant_quota` | metered API quota | `HSETANDGET` |
| 3 | `record_spend` + `pacing` | campaign pacing | `CONTROLSTATEINCROPT` + `HQUERY` |
| 4 | `rolling_suppression` | risk / spike suppression | `CONTROLSTATEINCROPT` + `HQUERY` |
| 5 | `ingest_impression` | replay-safe queue ingest | `HSETANDGETOPT` (uuid dedup) |
| 6 | `mark_seen` / `seen_value` | eligibility first/last | `FOLSET` / `FOLQUERY` |
| 7 | `rate_limit` | sliding-window throttle | `HSETANDGETOPT` (rolling window) |
| 8 | `debounce` | collapse bursts (quiet window) | `HSETANDGETOPT` (limit 1) |
| 9 | `concurrency_acquire` / `_release` / `concurrency_slot` | in-flight semaphore | `HSETANDGET` +1 / `CONTROLSTATEINCR` −1 |
| 10 | `add_unique` / `unique_count` | distinct-value counting | `CONTROLSTATECHANGE` + `change` query |
| 11 | `budget_gate` | hard spend/credit cap | `HSETANDGET` (amount = cost) |

All eleven are verified end-to-end over a real socket (exact cap enforcement,
idempotent replay/charging, concurrency rollback, debounce, distinct counting).
Run the built-in demo against a live endpoint with
`python -m temporalstore.control_state`.

---

## 6. Performance characterization

The freq-cap **decision correctness** is exact regardless of bucketing (a
range-sum over the window equals the exact count). The measured cost is
dominated by the **durable write path**: each `ControlStateSetAndGet` persists
its page and, on the live path, runs a per-command
`promote_model_maps_to_slot_index_authority` scan that is **O(live keyspace)**.
Over a growing keyspace this is the known O(n²) live-ingest cost that memory
records as solved **only for bulk-ingest mode** (deferred to a single
`flush_shard_index`).

The harness makes the cost visible: with the write set growing from ~90 to
~2,600 live keys within a single run, mean per-op latency climbs ~17×
(1.3 ms → 22.6 ms) even though each decision touches only a handful of
`BTreeMap` entries. The excess is the full-store
`collect_model_live_page_entries` + membership scan run after every command
(`engine.rs:211`). The pure in-memory decision (read + single-bucket update) is
sub-millisecond; the growth is the promotion scan plus the durable page append.

**Attempted fix (reverted).** Gating the promotion for self-registering
slot-index-direct commands looked safe (strictly less aggressive than bulk
mode) but **regressed read correctness** under async eviction (freq-cap counts
came back wrong) and did not improve latency — control-state reads depend on
the promotion/secondary-view reconcile for reload consistency. A correct
live-path deferral must carry the bulk-mode flush/reconcile machinery, so it is
recorded as a **scoped follow-up**, not claimed as done.

---

## 7. Verification

- **Control-state lib tests:** 7/7 pass (incl. the 2 new parity tests).
- **Full lib suite:** 521 pass. 11 failures are a **pre-existing baseline**
  (verified by running the identical tests at clean `HEAD` with all changes
  stashed — the same 11 fail there): cache-eviction soak, slot-dump, cold-index,
  storage-manager, and readiness gates, plus environment-dependent context
  embeddings. Two additional flaky failures seen once under concurrent build
  load (`http_raft_transport` = `EADDRINUSE` on a hardcoded port in `TIME_WAIT`;
  `resource_ingest` = live-embeddings) **pass in isolation** with the changes.
- **Net: the rename + new command introduce zero regressions.**

---

## 8. Follow-ups (⚠️)

1. **Live-path slot-index deferral** — extend the bulk-mode
   defer-promotion-then-`flush_shard_index` reconcile to the live control-state
   write path so high-QPS serving is O(1)/write instead of O(keyspace). (The
   naive skip regressed reads; needs the flush machinery.)
2. **Hierarchical precision rollup** — C++ writes each sample into both a fine
   and a coarse bucket and reads via a greedy multi-precision split; Rust uses a
   flat `BTreeMap` range. Only matters for very wide windows at fine precision.
3. **CPC distinct-count sketch** — Rust distinct counting is exact
   (`BTreeSet`); C++ upgrades to a probabilistic `cpc_sketch` at 4096 distinct
   for bounded memory on high-cardinality keys.
4. **gRPC surface** — expose the full control-state command set (currently 2 of
   14 mapped; RESP + native client are complete).
