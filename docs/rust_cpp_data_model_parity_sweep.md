# Rust/C++ Data Model Parity Sweep

## Scope

This pass treats Rust TemporalStore parity as behavioral parity through the shared C++/Rust corpus, not brpc/thrift wire compatibility or byte-for-byte C++ storage layout.

The executable sweep case is `all_data_model_cpp_rust_parity_sweep` in `compat/unified_temporalstore_cases.json`.

## Covered Model Families

| Family | Rust surface exercised | C++ parity target |
| --- | --- | --- |
| Common | `exists`, TTL-backed string write | common key lifecycle |
| String | `string_set_ex`, `string_get` | string model read/write |
| Hash | `hash_multi_set`, `hash_get_all` | hash object fields |
| Set | `set_add`, `set_members` | set membership |
| Feature | append, aggregate query | timestamp/value Feature pages |
| Sequence | C++ feature-row encoding, filtered query | sequence rows over Feature-style values |
| IPS | add with metadata, filter, stats | IPS instance metadata and range queries |
| Risk | H/CPC/FOL set/query | risk counter/list/FOL families |
| Context | node, extracted event, secondary indexes, entity, child ref, embedding, summary, compression | current C++ context sidecar model set |

## Remaining Deeper Parity Work

- C++ IPS has richer table-schema operators and ranking modes than the Rust-native executable sweep proves.
- C++ Risk still has broader thrift manager/window semantics; Rust covers the production Rust-native command contract and shared family behavior.
- Context parity is strongest for the current lean hot models and sidecars; live extraction/model-provider benchmark parity remains a separate context pipeline gate.
- Storage/Raft parity remains governed by the storage and Raft shared cases rather than this product-model sweep.

## Validation

Run:

```bash
python3 tools/run_temporalstore_unified_tests.py --validate-only
CARGO_TARGET_DIR=/tmp/temporalstore-feature-push-target cargo test -p temporalstore-rust --test unified_temporalstore_corpus -- --test-threads=1
```

