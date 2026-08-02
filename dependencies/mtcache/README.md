# MtCache: Multi-Tiered Cache

MtCache is a C++20 multi-tiered cache engine. A single logical cache spans
several storage tiers — DRAM, PMEM (persistent memory), and SSD — and moves
entries between tiers according to pluggable admission and eviction policies.
It exposes a small, zero-copy-friendly key/value interface and is consumed by
TemporalStore as a vendored dependency.

> Status: preparing for open-source distribution. The core library and tests
> build and run with permissive third-party dependencies; a small number of
> internal dependencies still gate a fully self-contained public build. See
> [`docs/mtcache_open_source_readiness.md`](../../docs/mtcache_open_source_readiness.md)
> for the ranked blocker list and de-internalization plan.

## Public API

The primary interface is `mtcache::Cache<Key, Value>` (`src/mtcache.h`):

```cpp
template <class Key, class Value>
class Cache {
 public:
  virtual bool Start() = 0;                    // start all tiers
  virtual bool Stop()  = 0;                    // stop all tiers
  virtual void Insert(const Key&, Value, size_t size = 1) = 0;
  virtual std::optional<Value> Lookup(const Key&) = 0;
  virtual void Remove(const Key&) = 0;
  virtual void RemoveAll() = 0;
  virtual size_t Capacity() const = 0;
  virtual void   SetCapacity(size_t) = 0;
  virtual size_t Size() const = 0;
};
```

`UnifiedCache` (`src/unified_cache.h`) implements a zero-copy variant over
`folly::IOBuf` values, backing a key/value entry with DRAM and PMEM cache
instances and a configurable data-placement policy. Lookups return a `Handle`
that keeps the underlying buffer pinned for the caller's lifetime, avoiding a
copy on the hot path.

## Source layout

| Path            | Contents                                                        |
| --------------- | --------------------------------------------------------------- |
| `src/mtcache.h` | Abstract `Cache` / `ZeroCopyCache` interfaces                   |
| `src/unified_cache.*` | Zero-copy DRAM+PMEM cache implementation                  |
| `src/cache_instance.*`, `src/cache_executor.*` | Per-tier instance & executor plumbing |
| `src/allocator/`| DRAM / log / jemalloc-backed allocators                         |
| `src/buffer/`   | Cache buffer types                                              |
| `src/map/`      | Index / hash-map structures                                     |
| `src/policy/`   | Admission, eviction, and tier-placement (L2) policies           |
| `src/storage/`  | SSD backends (zoned-store, TerarkDB), GC, multi-SSD             |
| `src/common/`, `src/util/` | Shared helpers                                       |
| `src/tools/`    | `cache_bench` and cache wrappers                                |
| `src/test/`     | Unit tests (run via `ctest`)                                    |
| `third_party/`  | CMake declarations for external dependencies                    |

## Building

Requirements: a C++20 compiler (GCC ≥ 10.2 or Clang 12+), CMake ≥ 3.12, and the
usual autotools/build toolchain. Third-party libraries are fetched and built
into `third_party/install*` on first build.

```bash
# From dependencies/mtcache/
./build.sh                                   # Release build, GCC
./build.sh --build-type Debug --enable-asan  # Debug + AddressSanitizer (Clang)
./build.sh --skip-test                       # library only, no tests/benchmarks
```

`build.sh` builds the third-party tree once (`third_party/`) and then the
`build/` tree. Run the tests with:

```bash
cd build && ctest --output-on-failure --timeout 300
```

### Relevant CMake options

| Option                 | Default | Purpose                                        |
| ---------------------- | ------- | ---------------------------------------------- |
| `MTCACHE_BUILD_TEST`   | `ON`    | Build unit tests, benchmarks, and tools        |
| `BUILD_SSD_CACHE`      | `ON`    | Build the SSD cache component                   |
| `ENABLE_ASAN`          | `OFF`   | AddressSanitizer (Debug only)                   |
| `ENABLE_CCACHE`        | `ON`    | Use ccache when available                       |
| `ENABLE_UNWIND`        | `OFF`   | Link libunwind                                  |

## How TemporalStore consumes MtCache

The parent repository wires MtCache in through two CMake options
(`CMakeLists.txt`):

- `ENABLE_MTCACHE` — defaults **OFF**. The core server/metaserver builds
  cleanly without MtCache; enable it only after MtCache's own dependencies are
  validated in your environment.
- `ENABLE_MTCACHE_SSD_CACHE` — defaults **OFF**. Adds the SSD/TerarkDB backend.

This staging exists precisely because MtCache's SSD/PMEM paths pull in the
internal dependencies noted below. A first-pass build should keep both OFF.

## Open-source caveats (internal dependencies)

Two dependencies are not yet publicly buildable and gate a fully self-contained
release:

- **bytedisk** — shipped only as an internal prebuilt binary
  (`third_party/externals/bytedisk.1.2.0.cmake`). Used by certain SSD/PMEM
  storage paths. No public source is available today.
- **noodle** — an internal metrics/concurrency library. The SSD and zoned-store
  engines include its metrics reporting headers
  (`noodle/metric/bytedance_metric_report_buidler.h`). Its dependency
  declaration currently points at a local download placeholder
  (`third_party/externals/noodle-v20210325.cmake`).

The Docker image and CI declarations under `docker/` and `.codebase/` reference
internal container registries and HTTP proxies and are **not** usable outside
that infrastructure. Treat them as historical references, not a public build
path; the plain `./build.sh` flow above does not depend on them.

The full plan for removing or gating these — so the DRAM/PMEM core builds
standalone and the SSD tier degrades gracefully — is tracked in
[`docs/mtcache_open_source_readiness.md`](../../docs/mtcache_open_source_readiness.md).

## License

Apache License 2.0. See [`LICENSE`](LICENSE) and [`NOTICE`](NOTICE). Bundled
third-party libraries remain under their own licenses.
