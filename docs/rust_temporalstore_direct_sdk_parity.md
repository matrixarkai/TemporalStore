# Rust TemporalStore Direct SDK Parity

## Status

The Rust `sdk/rust/temporalstore` crate now exposes the same direct C ABI
operations that the C++ SDK exports through `temporalstore_c_client.h` for the
core key/hash/set and MatrixArk batch append path.

The Rust direct SDK is a thin safe wrapper over the C ABI. It does not use the
Rust proxy or the retired record-log compatibility path for these operations.

## Rust Direct SDK Methods

| Area | Rust method | C ABI symbol |
|---|---|---|
| String write | `put_string` | `temporalstore_put_string` |
| String read | `get_string` | `temporalstore_get_string` |
| Delete | `delete_object` | `temporalstore_delete_object` |
| Expiry | `expire` | `temporalstore_expire` |
| TTL | `ttl` | `temporalstore_ttl` |
| Hash write | `hset` | `temporalstore_hset` |
| Hash read | `hget` | `temporalstore_hget` |
| Hash delete | `hdel` | `temporalstore_hdel` |
| Set add | `sadd` | `temporalstore_sadd` |
| Set members | `smembers` | `temporalstore_smembers` |
| MatrixArk batch append | `matrixark_batch_append_records` | `temporalstore_matrixark_batch_append_records` |
| Sequence feature write | `add_sequence_feature_rows` | `temporalstore_add_sequence_feature_rows` |
| Sequence feature query | `query_sequence_feature_rows` | `temporalstore_query_sequence_feature_rows` |

## Remaining Direct SDK Gaps

These are not Rust-wrapper gaps; they require the C++ C ABI to export additional
symbols first:

- hash scan / `HGetAll` through the C ABI;
- native prefix scan through the C ABI;
- native MatrixArk `retrieve_context_pack` through the C ABI.

Until those C ABI symbols exist, Rust can reach similar behavior through the Rust
native client/proxy paths, but the direct SDK cannot honestly claim identical
C++ direct SDK coverage for those APIs.

## Validation

```bash
cargo test --manifest-path sdk/rust/temporalstore/Cargo.toml --no-default-features --features proxy
cargo check --manifest-path sdk/rust/temporalstore/Cargo.toml --lib --tests
```

The direct test includes a compile-time API surface check for the C ABI parity
methods. The direct `cargo check` path compiles that surface without requiring a
local `libbcache2` link. When the C++ SDK release bundle is available, run the
full direct linked test:

```bash
TEMPORALSTORE_LIB_DIR=/path/to/sdk/lib \
  cargo test --manifest-path sdk/rust/temporalstore/Cargo.toml
```

The proxy test validates that the crate can still compile without the direct
native library link path.
