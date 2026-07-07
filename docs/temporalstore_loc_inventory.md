# TemporalStore Line Of Code Inventory

Date: 2026-07-07

This document records the local source line counts for TemporalStore and the key local dependency trees used by the C++/Rust TemporalStore work.

## Counting Method

The counts below use nonblank source lines as the primary LOC number.

Included:

- maintained source files;
- tests and examples inside source trees;
- C/C++ headers and implementation files;
- Rust crates;
- Python tools/scripts;
- protobuf definitions.

Excluded:

- `.git`, build, output, target, cache, and generated artifact directories;
- `thirdparty`, `.local`, and vendored dependency trees when counting first-party TemporalStore;
- docs benchmark/debug artifacts, logs, and bulky generated reports.

## First-Party TemporalStore

| Component | Path | Files | Total lines | Nonblank LOC |
|---|---:|---:|---:|---:|
| TemporalStore C++ first-party | `/root/src/github-services/TemporalStore/src` | 525 | 126,663 | 112,139 |
| TemporalStore Rust first-party | `/root/src/github-services/TemporalStore/crates` | 96 | 144,510 | 137,949 |
| TemporalStore Python first-party | `/root/src/github-services/TemporalStore/tools`, `scripts`, `src`, `crates` | 149 | 88,787 | 81,348 |
| TemporalStore Proto | `/root/src/github-services/TemporalStore/src`, `crates`, `tools` | 24 | 3,481 | 2,933 |

First-party total, C++ + Rust + Python + Proto: **334,369 nonblank LOC**.

## Dependency Trees

| Dependency | Path | Files | Total lines | Nonblank LOC |
|---|---:|---:|---:|---:|
| byteraft C++ dependency | `/root/src/github-services/TemporalStore/.local/deps-src/byteraft-master` | 210 | 50,287 | 41,996 |
| byte C++ dependency | `/root/src/github-services/TemporalStore/.local/deps-src/byte-master` | 377 | 85,916 | 74,964 |
| RustRaft external lib | `/root/src/github-services/RustRaft` | 63 | 36,819 | 34,530 |
| rustmtcache external lib | `/root/src/github-services/rustmtcache` | 2 | 24,790 | 22,003 |
| mtcache C++ dependency | `/root/TemporalStore-main-slice5/dependencies/mtcache` | 145 | 31,099 | 26,615 |

Listed dependency total: **200,108 nonblank LOC**.

## Rollups

| Rollup | Nonblank LOC |
|---|---:|
| TemporalStore first-party C++ + Rust + Python + Proto | 334,369 |
| Listed dependencies total | 200,108 |
| TemporalStore C++ plus C++ dependencies: byteraft + byte + mtcache | 255,714 |
| Rust ecosystem dependencies: RustRaft + rustmtcache | 56,533 |

## Notes

- `thirdparty/byte` points to `.local/deps-src/byte-master`.
- `thirdparty/byteraft` points to `.local/deps-src/byteraft-master`.
- The Rust count includes large maintained files such as `engine.rs`, `raft.rs`, `redis.rs`, server/metaserver/proxy binaries, and tests.
- The Python count includes MatrixArk MCP/core adapter code, backfill tooling, benchmark/report scripts, validation scripts, and `cpplint.py`.
- These numbers are local-source inventory numbers, not a statement of what is linked into any single final binary.
