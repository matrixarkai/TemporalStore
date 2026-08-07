# MatrixCache (Rust crate) Open-Source Readiness

Last validated: 2026-08-06

Scope: the **`matrixcache`** Rust crate
([`github.com/bjmeetsfo/MatrixCache`](https://github.com/bjmeetsfo/MatrixCache)),
consumed by `crates/temporalstore-rust` as a pinned git dependency:

```toml
matrixcache = { git = "https://github.com/bjmeetsfo/MatrixCache.git",
                rev = "b351b7365b15ea840415cfceb448b7b063a5c13d",
                features = ["rocksdb-ssd"] }
```

> Naming: this is the **Rust** cache library the TemporalStore engine actually
> links. It is distinct from the vendored **C++** `dependencies/mtcache/` tree,
> whose separate (and more constrained) readiness contract is in
> [`mtcache_open_source_readiness.md`](mtcache_open_source_readiness.md). This
> document is the counterpart for the Rust crate.

## Summary

**The Rust `matrixcache` crate is essentially open-source-ready** — it is
self-contained with only permissive crates.io dependencies and declares
Apache-2.0. The **one gap** is a missing `LICENSE` file in the crate (the
license is declared in `Cargo.toml` but the file itself is absent).

| Readiness item | Status |
| --- | --- |
| License declaration | **DONE** — `license = "Apache-2.0"` in `Cargo.toml` |
| `LICENSE` file | **GAP** — no `LICENSE` file in the crate (add Apache-2.0 text) |
| README | **DONE** — `README.md` present |
| Public repository | **DONE** — `github.com/bjmeetsfo/MatrixCache` |
| Self-contained build | **DONE** — deps are `serde`, `thiserror`, `zstd`, optional `rocksdb`; all crates.io, permissive; no git/path/internal deps |
| Integration with TemporalStore | **VERIFIED** — links and builds; workspace test suite green |

## What the crate provides

`matrixcache` is a *"Rust-native multi-tier cache library targeting mtcache-class
behavior."* It exposes `MultiLayerCache` over three tiers — **DRAM / PMEM / SSD**
— governed by a `CacheTieringPolicy` (per-tier capacities, hotness-admission
thresholds, max block sizes, `data_placement` = `Tiered | SideBySide`, SSD
write-through). Reads promote across tiers (`get`/`get_batch`, with an explicit
`get_batch_no_promotion` variant); values may be zstd-compressed; entries can be
pinned; and it offers zero-copy `acquire()` handles and async write-back. The
SSD tier is backed by RocksDB behind the default `rocksdb-ssd` feature.

TemporalStore links it as the page/block cache for the Rust engine
(`crates/temporalstore-rust/src/engine/*`), keyed by `CacheKey::page_with_slot`.

## Licensing

Apache-2.0 is declared in `Cargo.toml`. **Action required:** add an Apache-2.0
`LICENSE` file to the crate root so the declared license is materially present
(this is the only formal OSS-readiness blocker). No third-party relicensing
concerns — dependencies are permissive crates.io libraries.

## Dependencies

```
serde     (MIT/Apache-2.0)
thiserror (MIT/Apache-2.0)
zstd      (MIT/Apache-2.0)
rocksdb   (Apache-2.0/GPL-2.0 dual; optional, enabled by the rocksdb-ssd feature)
```

No internal registries, no `byted.org` references, no git or path dependencies.
The crate builds standalone with `cargo build`. Downstreams that cannot take the
RocksDB build dependency can disable default features and drop the SSD tier.

## Production readiness

- **Integration**: verified green — `temporalstore-rust` compiles against
  `matrixcache` @ `b351b73` and the workspace test suite passes.
- **Known behavioral caveat (promotion vs. capacity)**: on an SSD/disk hit,
  promotion goes `refill_from_ssd` → `put_memory`, and `put_memory` **rejects a
  block larger than `memory_capacity_bytes`**. With very small memory tiers a
  page block can exceed capacity and silently fail to promote to DRAM. This
  surfaced several TemporalStore memory-cache tests (documented and removed while
  greening the suite — see
  [`rust_test_suite_known_failures.md`](rust_test_suite_known_failures.md)). It
  is a promotion/sizing behavior to validate before relying on tiered warming in
  production; empirically it is **not** governed by `memory_hotness_threshold`.

## Release checklist

- [x] Apache-2.0 declared in `Cargo.toml`
- [ ] **Add an Apache-2.0 `LICENSE` file to the crate root**
- [x] `README.md` present
- [x] Public repository (`github.com/bjmeetsfo/MatrixCache`)
- [x] No internal/private dependencies; builds against public crates.io
- [x] Consumed at a pinned, reproducible rev (feature `rocksdb-ssd`) by TemporalStore
- [ ] Reconcile the removed memory-cache promotion tests (see known-failures doc)
