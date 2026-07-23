# TemporalStore Multi-language SDKs

TemporalStore has two SDK families:

- Direct SDKs call the native client or stable C ABI and route to data nodes.
- Proxy SDKs are pure-language HTTP clients that call a TemporalStore proxy.

Direct SDKs are fastest. Proxy SDKs are easiest to ship to customers because they
avoid native library loading and centralize auth, quotas, topology, retry, and
observability in the proxy.

## Layout

| Language | Path | Binding strategy |
|---|---|---|
| C++ | `sdk/cpp` and `src/client/temporalstore_client.h` | Native SDK |
| C | `src/client/temporalstore_c_client.h` | Stable ABI |
| Go direct | `sdk/go/temporalstore/client.go` | cgo wrapper over C ABI |
| Go proxy | `sdk/go/temporalstore/proxy_client.go` | pure HTTP client |
| Java direct | `sdk/java/temporalstore/.../TemporalStoreClient.java` | JNA wrapper over C ABI |
| Java proxy | `sdk/java/temporalstore/.../TemporalStoreProxyClient.java` | pure HTTP client |
| Python direct | `sdk/python/temporalstore/client.py` | `ctypes` wrapper over C ABI |
| Python proxy | `sdk/python/temporalstore/proxy_client.py` | pure HTTP client |
| Rust direct | `sdk/rust/temporalstore` | FFI wrapper over C ABI |
| Rust proxy | `sdk/rust/temporalstore` | pure HTTP client |

## Shared Library

Build the native shared library:

```bash
cmake --build build-ubuntu22 --target bcache2-shared -j4
```

The local Release build writes:

```bash
output/sdk/lib/libbcache2.so
```

Some Debug builds may write `output/sdk/lib/libbcache2d.so`; the smoke script
detects whichever one exists.

Release packaging should publish the shared object plus public headers from
`src/client/temporalstore_c_client.h` and `sdk/cpp/include`.

## Covered Data Types

The Go, Java, and Python wrappers now expose the same C ABI surface:

- connect / close
- STRING: `put_string` / `get_string`
- COMMON: `put_string_with_ttl`, `delete_object`, `expire`, `ttl`
- HASH: `hset` / `hget` / `hdel`
- SET: `sadd` / `smembers`
- FEATURE: raw feature point add/query with filters
- SEQUENCE FEATURE: typed long-sequence row add/query with filters
- IPS: add instance and query last instances
- RISK: increment and window count

The C++ native client is still the fullest surface because it also exposes lower
level native request controls.

## Python

```bash
export TEMPORALSTORE_LIB=/path/to/libbcache2.so
export LD_LIBRARY_PATH=/path/to/libdir:$LD_LIBRARY_PATH
export PYTHONPATH=/path/to/repo/sdk/python
python3 sdk/python/examples/sequence_features.py
```

For the local Debug build, set `TEMPORALSTORE_LIB=/path/to/libbcache2d.so`.

Python proxy SDK:

```bash
export PYTHONPATH=/path/to/repo/sdk/python
python3 sdk/python/examples/proxy_sequence_features.py
```

## Go

```bash
cd sdk/go/temporalstore
export LD_LIBRARY_PATH=/path/to/libdir:$LD_LIBRARY_PATH
export CGO_LDFLAGS="-L/path/to/libdir -lbcache2"
go test -tags temporalstore_direct ./...
go run -tags temporalstore_direct ./examples
go run ./examples/proxy
```

For the local Debug build, use `CGO_LDFLAGS="-L/path/to/libdir -lbcache2d"`.

The Go direct SDK is behind the `temporalstore_direct` build tag so the Go proxy
SDK remains a pure HTTP client with no cgo/native dependency.

## Java

```bash
cd sdk/java/temporalstore
mvn package
export LD_LIBRARY_PATH=/path/to/libdir:$LD_LIBRARY_PATH
```

Set `Options.libraryName = "bcache2d"` when using the local Debug library.

The same package also includes `TemporalStoreProxyClient`, which only needs Java
11 HTTP client plus Jackson.

## C++

```cpp
#include "temporalstore/client.h"
```

The convenience header aliases the native SDK into the `temporalstore` namespace.

## Rust

Direct SDK:

```bash
cd sdk/rust/temporalstore
export LD_LIBRARY_PATH=/path/to/libdir:$LD_LIBRARY_PATH
export TEMPORALSTORE_LIB_DIR=/path/to/libdir
export TEMPORALSTORE_LIB_NAME=bcache2
cargo run --example sequence_features
```

Proxy SDK without native linking:

```bash
cd sdk/rust/temporalstore
cargo run --no-default-features --features proxy --example proxy_sequence_features
```

## Proxy API

The proxy API contract is in `sdk/proxy/openapi.yaml`. More design detail is in
`docs/direct_vs_proxy_sdk.md`.

## Smoke Tests

Direct SDK smoke against a local metaserver/server cluster:

```bash
RUN_PYTHON_SDK=1 RUN_GO_SDK=1 RUN_JAVA_SDK=1 RUN_RUST_SDK=1 \
  TEMPORALSTORE_PYTHON_LIB=/path/to/libbcache2.so \
  tools/run_sdk_smoke_ubuntu22.sh
```

Proxy SDK smoke against a mock HTTP gateway:

```bash
tools/run_proxy_sdk_smoke_ubuntu22.sh
```

The SDK shared library must stay client-only. Do not whole-archive link storage
modules into `bcache2-shared`; doing that pulls server-side global initializers
and TLS-heavy code into customer runtimes.
