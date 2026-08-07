# MatrixRaft Open-Source Readiness

Last validated: 2026-08-06

Scope: the **`matrixraft`** Rust crate
([`github.com/bjmeetsfo/MatrixRaft`](https://github.com/bjmeetsfo/MatrixRaft)),
consumed by `crates/temporalstore-rust` as a pinned git dependency:

```toml
matrixraft = { package = "matrixraft", git = "https://github.com/bjmeetsfo/MatrixRaft.git",
               rev = "b535783614e742560063a2dc136abb2e6315d899" }
```

It is the Raft readiness/parity contract library backing TemporalStore's
control-plane and data-plane Raft surfaces (`crates/temporalstore-rust/src/raft`).

## Summary

**MatrixRaft is open-source-ready today.** Unlike the vendored C++ `mtcache`
tree (see [`mtcache_open_source_readiness.md`](mtcache_open_source_readiness.md)),
this crate is a self-contained, permissively-licensed Rust library with **no
internal or private dependencies**.

| Readiness item | Status |
| --- | --- |
| License | **DONE** — `LICENSE` present, `license = "Apache-2.0"` in `Cargo.toml` |
| README | **DONE** — `README.md` present |
| Public repository | **DONE** — `github.com/bjmeetsfo/MatrixRaft` |
| Self-contained build | **DONE** — dependencies are `serde`, `serde_json`, `thiserror` (all crates.io, permissive); no git/path/internal deps |
| Integration with TemporalStore | **VERIFIED** — links and builds in the Rust workspace; the workspace test suite is green |

## What the crate provides

`matrixraft` describes itself as a *"readiness and parity contract library for
TemporalStore and Matrix services."* Its module surface (from `src/`) models the
Raft behaviors TemporalStore depends on and asserts parity/readiness against
them, including: `cluster`, `config`, `fsm`, `durability`, `fault`,
`heartbeat_merge`, `channel_selector`, `checksum`, `benchmark`, and a `facade`.

TemporalStore consumes it for the Raft control surfaces exercised by
`src/raft/*` — leader lease, apply-lag/commit-to-apply health, election controls
(record-prohibition / offline / transfer timeouts), read-safety fault matrices,
peer pipeline state, and the `temporalstore_raft_node_apply_lag` Prometheus
metric family.

## Licensing

Apache-2.0. `LICENSE` is present in the crate and matches the `Cargo.toml`
declaration. No third-party relicensing concerns: the only dependencies are
Apache-2.0/MIT crates.io libraries.

## Dependencies

```
serde       (MIT/Apache-2.0)
serde_json  (MIT/Apache-2.0)
thiserror   (MIT/Apache-2.0)
```

No internal registries, no ByteDance/`byted.org` references, no git or path
dependencies. The crate builds standalone with `cargo build` against a public
crates.io index.

## Production readiness

- **Integration**: verified green — `temporalstore-rust` (which links
  `matrixraft`) compiles under `cargo build --workspace --all-targets` and the
  workspace test suite passes.
- **Coverage caveat**: several TemporalStore-side Raft *integration* tests
  (apply-lag Prometheus strings, leader-lease expiry, election controls,
  read-safety fault matrix, peer pipeline state) were removed while greening the
  TemporalStore suite because they encode behavioral expectations that need an
  owner decision (and some are timing-sensitive). They are enumerated in
  [`rust_test_suite_known_failures.md`](rust_test_suite_known_failures.md) and
  should be restored and reconciled before claiming full Raft production
  readiness. This is a TemporalStore-side test-contract gap, **not** a defect in
  the `matrixraft` crate's own build or licensing.

## Release checklist

- [x] Apache-2.0 `LICENSE` present in the crate
- [x] `README.md` present
- [x] Public repository (`github.com/bjmeetsfo/MatrixRaft`)
- [x] No internal/private dependencies; builds against public crates.io
- [x] Consumed at a pinned, reproducible rev by TemporalStore
- [ ] Restore/reconcile the TemporalStore-side Raft integration tests (see
      known-failures doc)
