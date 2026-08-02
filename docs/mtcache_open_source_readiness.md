# MtCache Open-Source Readiness

Last validated: 2026-08-03

Scope: `dependencies/mtcache/` — the vendored C++20 multi-tiered (DRAM / PMEM /
SSD) cache engine. This document records the open-source and production-
readiness contract for that component, mirroring the repository-level contract
in [`open_source_readiness.md`](open_source_readiness.md), which today covers
only the Rust workspace and does not mention MtCache.

## Naming: two "MatrixCache" surfaces

To avoid confusion, this repository exposes cache functionality through two
distinct artifacts:

- **`dependencies/mtcache/`** — the vendored **C++20** cache engine documented
  here. Gated OFF by default in the parent build (`ENABLE_MTCACHE`).
- **`matrixcache` (Rust crate)** — an external dependency pulled by
  `crates/temporalstore-rust` from
  `https://github.com/bjmeetsfo/MatrixCache.git` (pinned rev
  `b351b7365b15ea840415cfceb448b7b063a5c13d`, feature `rocksdb-ssd`). This is
  what the Rust engine actually links against today. Its integration is
  **verified**: `temporalstore-rust` (lib) and `temporalstore-snapshot` compile
  and link against it cleanly (see [Rust integration status](#rust-integration-status)).

The blockers below concern the **vendored C++ tree only**; they do not affect
the Rust crate integration.

## Summary

MtCache's core (the `Cache` / `UnifiedCache` interfaces, allocators, buffers,
maps, policies, and DRAM/PMEM tiers) is clean, permissively licensed C++. The
blockers to a self-contained public build are concentrated in **one hard
dependency (noodle)** plus a set of **internal build/CI references** that are
documentation-only and safe to replace. The SSD-tier internal dependency
(bytedisk) is already gated OFF by default.

Readiness now:

- Licensing: **DONE** — `LICENSE` (Apache-2.0) and `NOTICE` added; `README.md`
  documents API, build, and caveats.
- Self-contained build: **BLOCKED** on noodle (see B1).
- CI / container: **NOT PUBLIC** — internal registry/proxy references only.

## Required public files (component-level)

| File                        | Status |
| --------------------------- | ------ |
| `dependencies/mtcache/LICENSE` | present (Apache-2.0) |
| `dependencies/mtcache/NOTICE`  | present |
| `dependencies/mtcache/README.md` | present (real docs) |

## Blockers, ranked

### B1 — `noodle` is an unconditional internal dependency (HARD)

`CMakeLists.txt:79` calls `find_package(Noodle REQUIRED)` at top level, and
`src/CMakeLists.txt:101` links `${NOODLE_LIBRARY}` unconditionally. Noodle is an
internal metrics/concurrency library; its metrics reporting header
(`noodle/metric/bytedance_metric_report_buidler.h`) is included by **10 source
files**, all in the SSD / zoned-store / tools paths:

```
src/allocator/je_allocator.h        src/storage/multi_ssd.h
src/allocator/log_allocator.h       src/storage/ssd_terarkdb.h
src/cache_instance.h                src/storage/zoned_store/metrics.h
src/storage/gc_controller.h         src/tools/cache_bench.h
                                    src/tools/cache_wrapper.h
```

Its dependency declaration (`third_party/externals/noodle-v20210325.cmake`)
already points at a `<local-downloads>/noodle.zip` placeholder rather than an
internal URL, so it cannot be fetched publicly today.

Options (recommended first):

1. **Metrics shim.** Add a small open-source header that provides the metrics
   reporting surface MtCache actually uses (a no-op / pluggable reporter),
   selected when noodle is absent. Lowest blast radius; keeps the 10 call sites
   unchanged behind an include shim.
2. **Make noodle optional.** Introduce `ENABLE_NOODLE_METRICS` (default OFF),
   guard `find_package(Noodle)` and the 10 include sites with `#ifdef`, and
   compile a metrics no-op otherwise. More invasive but removes the dependency
   entirely from the default build.
3. **Publish noodle.** Out of scope unless the upstream library is itself
   open-sourced.

Until B1 is resolved, the default `./build.sh` cannot complete in a public
environment.

### B2 — Internal container image and HTTP proxies (MEDIUM, doc-only)

Not linked into the library; only affect the container build path.

- `docker/Dockerfile` — base image `hub.byted.org/compile/debian.stretch...`,
  proxies `sys-proxy-rd-relay.byted.org:8118` / `bj-rd-proxy.byted.org:3128`,
  and internal `/opt/tiger` + `tiger` user conventions.
- `.codebase/pipelines/ci.yaml` — ByteDance Codebase CI, internal image
  `hub.byted.org/compile/mtcache_compile_clang:...`, links to Feishu wiki docs.
- `third_party/README.md` — instructs pulling the internal `hub.byted.org`
  compile image.

Plan: replace the Dockerfile base with a public image (e.g. `debian:bookworm`
or `ubuntu:22.04`), drop the internal proxy exports, drop `/opt/tiger` tiger-
user setup, and (when a credential with GitHub `workflow` scope is available —
see the repo-level readiness note) add a `.github/workflows/` job that runs
`./build.sh --skip-test` + `ctest`. Retire or clearly mark `.codebase/` as
non-public. The plain `./build.sh` flow already does **not** need any of these.

### B3 — `bytedisk` prebuilt binary (LOW, already gated)

`third_party/externals/bytedisk.1.2.0.cmake` downloads a prebuilt binary from
`tosv.byted.org` (no public source; the file's own TODO notes source build is
not yet possible), resolved via `cmake/FindBytedisk.cmake`. It is only pulled in
when `BUILD_ZONED_STORE=ON`, which **defaults OFF** (`CMakeLists.txt:31`, gated
at `CMakeLists.txt:112`). No action required for a default OSS build; document
that the Bytedisk-backed ZonedStore engine is unavailable publicly until
bytedisk has a public source or the engine is reworked onto a public block
device layer.

### B4 — Internal comments / identifiers in source (LOW, cosmetic)

Non-functional references: a ByteDance-PMem hardware comment in
`src/allocator/alloc_utils.cpp`, `@bytedance.com` author tags in TODOs
(`src/test/unified_cache_ssd_only_test.cpp`), and `WITH_BYTEDANCE_METRICS=OFF`
in `third_party/externals/terarkdb-dev.1.4.cmake` (already disabled — good).
Optional cleanup pass; no build impact.

## Production readiness

Beyond the OSS build, before MtCache is claimed production-ready in the open:

- Tests: `ctest` green in a public toolchain once B1 is resolved. The CI also
  runs the `unified_cache` / `unified_cache_ssd_only` suites a second time with
  `FLAGS_ssd_engine_type=0` (TerarkDB) — preserve both matrix legs publicly.
- Sanitizers: ASan is already a first-class Debug path (`--enable-asan`); keep
  it in CI.
- Degradation: the DRAM/PMEM core must build and pass tests with the SSD tier
  (TerarkDB / ZonedStore) disabled, so downstream OSS users can adopt the
  in-memory tiers without internal storage deps.

## Rust integration status

The Rust engine consumes the **external** `matrixcache` crate (not this vendored
C++ tree). As of the Last validated date above, on the current `main`:

- `crates/temporalstore-rust` (lib) and `crates/temporalstore-snapshot` build
  offline against `matrixcache` @ `b351b7365b15ea840415cfceb448b7b063a5c13d`
  (feature `rocksdb-ssd`) with only dead-code warnings — the library-level
  integration is **green**.
- Known caveat (pre-existing, not an integration regression): the workspace's
  `#[cfg(test)]` targets and the `matrixark_rust_direct_sdk` binary do not yet
  compile (`cargo build --workspace --all-targets` fails on drifted test-only
  struct fields, a duplicate test fn, and a `matrixcache::CacheTieringPolicy`
  field addition). These are tracked separately from OSS readiness; the shipping
  library path is unaffected.

## Validation

Suggested component gate (extend `tools/validate_open_source_readiness.py` to
cover `dependencies/mtcache/` once B1/B2 land):

```bash
# license/notice/readme present
test -f dependencies/mtcache/LICENSE
test -f dependencies/mtcache/NOTICE
# no internal registry/proxy refs in the public build path
! grep -rq "byted.org" dependencies/mtcache/build.sh dependencies/mtcache/CMakeLists.txt \
    dependencies/mtcache/src dependencies/mtcache/third_party/externals
# default build (after B1) — DRAM/PMEM core, SSD tier off
cd dependencies/mtcache && ./build.sh --build-type Debug --skip-test
```

## Release hygiene

Consistent with the repo-level contract: keep secrets and generated build
outputs (`build/`, `third_party/install*`, `_build/`) out of git; keep internal
CI/container references out of the public build path; tie production-readiness
claims to passing test evidence; and update this document when scope changes.
