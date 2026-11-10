// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A HASH FIELD WRITE FILES THE FIELD IT WROTE, NOT THE FIELDS THE HASH ALREADY HAD.
//!
//! A single-command hash write ended with a whole-object index sync that re-filed EVERY field the
//! hash held. That sync was given a filter -- "only the fields whose block is not already filed" --
//! so the ordinary case would cost nothing, and the filter asks a lookup that resolves no named
//! element name (`names_the_only_slot`: a `Some` element name has always found nothing). So it never
//! filtered, and one field write cost the fields the hash already had:
//!
//! ```text
//!   fields already in the hash        allocations per overwrite
//!                10                            118
//!               100                            571
//!             1,000                          5,080
//! ```
//!
//! 87% of the write at 100 fields. The batch path files only the element names a command wrote and has
//! done since it existed; the single-command path now does the same for a hash write.
//!
//! TWO ARMS, BECAUSE THEY GUARD DIFFERENT FAILURES.
//!   * `a_hash_write_leaves_every_field_filed_exactly_once` runs in a default build. Narrowing the
//!     sync must not leave a field unfiled or a stale block behind, and nothing else in the suite
//!     compares the hash's own addresses with the index's.
//!   * `a_hash_field_write_does_not_pay_for_the_fields_it_already_has` needs `alloc-probe` and
//!     `--ignored`. It varies the FIELD COUNT, because only work proportional to the fields already
//!     held can make the per-write count depend on it, and it carries its own control: a read of
//!     the whole hash is proportional to the fields by construction, so the same probe must see
//!     growth there or it is blind to the dimension.
//!
//! rust-internal: measures this crate's own request paths, no product behaviour

#![allow(clippy::all)]
use super::*;

use std::collections::BTreeSet;

fn engine_on(dir: &std::path::Path) -> Arc<TemporalEngine> {
    let engine = Arc::new(TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.join("cache"),
        dir.join("blocks"),
        dir.join("indexes"),
    ));
    engine.load_shard(1);
    engine
}

fn run(engine: &TemporalEngine, command: Command) {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command,
    });
    assert!(response.status.ok, "{:?}", response.status);
}

fn identity(address: &crate::ElementEntry) -> String {
    format!(
        "{:?}/{:?}/{:?}/{:?}",
        address.block_slab_id(),
        address.offset(),
        address.length(),
        address.block_id()
    )
}

#[test]
fn a_hash_write_leaves_every_field_filed_exactly_once() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());

    for object in 0..3 {
        let key = format!("h-{object}");
        for field in 0..40 {
            run(
                &engine,
                Command::HashSet {
                    key: key.clone(),
                    field: format!("f{field:03}"),
                    value: vec![b'a'; 16],
                },
            );
        }
        // Overwrites replace a field's block; a stale one left behind would show as a surplus.
        for field in 0..10 {
            run(
                &engine,
                Command::HashSet {
                    key: key.clone(),
                    field: format!("f{field:03}"),
                    value: vec![b'b'; 24],
                },
            );
        }
        run(
            &engine,
            Command::HashMultiSet {
                key: key.clone(),
                entries: (40..46)
                    .map(|field| (format!("f{field:03}"), vec![b'c'; 8]))
                    .collect(),
            },
        );
        for _ in 0..3 {
            run(
                &engine,
                Command::HashIncrBy {
                    key: key.clone(),
                    field: "counter".to_string(),
                    increment: 1,
                },
            );
        }
    }

    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard is loaded");
    assert!(
        !shard.bucket_index.bucket_map.is_empty(),
        "no bucket index was built, so the arm that narrows the sync never ran"
    );

    let mut checked_fields = 0usize;
    for object in 0..3 {
        let key = format!("h-{object}");
        let fields = shard.bucket_index.hashes.get(key.as_str()).expect("the hash exists");
        let held: BTreeSet<String> = fields.iter().map(|(_, address)| identity(address)).collect();

        let refs = shard
            .bucket_index
            .object_block_refs("hash", &key)
            .expect("the hash has block refs");
        let mut filed: Vec<String> = Vec::new();
        for block_ref in refs.iter() {
            let block = shard
                .bucket_index
                .bucket_map
                .get(&block_ref.routing_bucket)
                .and_then(|bucket| bucket.block_index.get(&block_ref.block_ref_key))
                .expect("a ref names a block the index holds");
            if !block.deleted() {
                filed.push(identity(&block.address));
            }
        }
        let filed_set: BTreeSet<String> = filed.iter().cloned().collect();

        // DENOMINATOR: 40 + 6 + 1 fields. A run that wrote fewer cannot have exercised the sync.
        assert_eq!(47, held.len(), "{key}: the hash holds {} distinct blocks", held.len());
        assert_eq!(
            held, filed_set,
            "{key}: the blocks the hash holds and the blocks the index files differ"
        );
        assert_eq!(
            filed.len(),
            filed_set.len(),
            "{key}: a block is filed twice, so a replaced field left its predecessor behind"
        );
        checked_fields += held.len();
    }
    assert_eq!(141, checked_fields);
}

#[test]
fn a_new_packed_hash_files_one_block_for_all_fields() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    run(
        &engine,
        Command::HashMultiSet {
            key: "packed".to_string(),
            entries: vec![
                ("first".to_string(), b"one".to_vec()),
                ("second".to_string(), b"two".to_vec()),
                ("payload".to_string(), vec![b'x'; 8 * 1024]),
            ],
        },
    );

    {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("shard is loaded");
        let fields = shard.bucket_index.hashes.get("packed").expect("hash exists");
        assert_eq!(fields.len(), 3);
        let addresses: BTreeSet<_> = fields.iter().map(|(_, address)| identity(address)).collect();
        assert_eq!(addresses.len(), 1);
        assert_eq!(shard.bucket_index.object_block_refs("hash", "packed").unwrap().len(), 1);
    }

    run(
        &engine,
        Command::HashSet {
            key: "packed".to_string(),
            field: "first".to_string(),
            value: b"updated".to_vec(),
        },
    );
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::HashGetAll { key: "packed".to_string() },
    });
    assert!(response.status.ok, "{:?}", response.status);
    let CommandResponse::HashEntries { entries } = response.response else {
        panic!("expected hash entries");
    };
    let values = entries.into_iter().collect::<std::collections::BTreeMap<_, _>>();
    assert_eq!(values.get("first"), Some(&b"updated".to_vec()));
    assert_eq!(values.get("second"), Some(&b"two".to_vec()));
    assert_eq!(values.get("payload").map(Vec::len), Some(8 * 1024));
}

#[cfg(feature = "alloc-probe")]
mod probe {
    use super::*;
    use crate::alloc_probe::Probe;

    const OPS: usize = 100;

    fn hash_of(engine: &TemporalEngine, fields: usize) {
        for field in 0..fields {
            run(
                engine,
                Command::HashSet {
                    key: "h".to_string(),
                    field: format!("f{field:05}"),
                    value: vec![b'v'; 64],
                },
            );
        }
    }

    /// Allocations per operation of `op`, built before the span opens.
    fn per_op(fields: usize, op: impl Fn(usize) -> Command) -> f64 {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = engine_on(dir.path());
        hash_of(&engine, fields);
        let commands: Vec<Command> = (0..OPS).map(op).collect();
        let probe = Probe::start();
        for command in commands {
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command,
            });
            assert!(response.status.ok);
        }
        probe.stop().allocs as f64 / OPS as f64
    }

    #[test]
    #[ignore = "measurement: run with --features alloc-probe --ignored"]
    fn a_hash_field_write_does_not_pay_for_the_fields_it_already_has() {
        const SMALL: usize = 10;
        const LARGE: usize = 400;

        // THE CONTROL, FIRST. Reading the whole hash is proportional to its fields by
        // construction. If this probe cannot see that, it cannot see the write's dependence
        // either and a flat answer below would mean nothing.
        let read_small = per_op(SMALL, |_| Command::HashGetAll {
            key: "h".to_string(),
        });
        let read_large = per_op(LARGE, |_| Command::HashGetAll {
            key: "h".to_string(),
        });
        println!("control  hash read-all: {read_small:.1} at {SMALL} fields, {read_large:.1} at {LARGE}");
        assert!(
            read_large > read_small * 10.0,
            "the control did not grow with the field count ({read_small:.1} -> {read_large:.1}), \
             so this probe is blind to that dimension"
        );

        let over_small = per_op(SMALL, |j| Command::HashSet {
            key: "h".to_string(),
            field: format!("f{:05}", j % SMALL),
            value: vec![b'w'; 64],
        });
        let over_large = per_op(LARGE, |j| Command::HashSet {
            key: "h".to_string(),
            field: format!("f{:05}", j % SMALL),
            value: vec![b'w'; 64],
        });
        let new_small = per_op(SMALL, |j| Command::HashSet {
            key: "h".to_string(),
            field: format!("n{j:05}"),
            value: vec![b'w'; 64],
        });
        let new_large = per_op(LARGE, |j| Command::HashSet {
            key: "h".to_string(),
            field: format!("n{j:05}"),
            value: vec![b'w'; 64],
        });
        println!("overwrite: {over_small:.1} at {SMALL} fields, {over_large:.1} at {LARGE}");
        println!("new field: {new_small:.1} at {SMALL} fields, {new_large:.1} at {LARGE}");

        // Before the fix: 118 -> 2,300 and 264 -> 2,400. Allowed slack is a handful, not a
        // fraction of the per-field cost (about 5.7 a field), so a surviving per-field term of
        // even one allocation fails.
        assert!(
            over_large - over_small < 8.0,
            "an overwrite cost {over_small:.1} allocations at {SMALL} fields and {over_large:.1} \
             at {LARGE}: it still pays for the fields the hash already had"
        );
        assert!(
            new_large - new_small < 8.0,
            "a new field cost {new_small:.1} allocations at {SMALL} fields and {new_large:.1} at \
             {LARGE}: it still pays for the fields the hash already had"
        );
    }
}
