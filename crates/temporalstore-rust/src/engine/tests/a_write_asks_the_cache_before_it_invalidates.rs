// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A WRITE ASKS THE CACHE BEFORE IT INVALIDATES, AND INVALIDATES WHAT IS THERE.
//!
//! A string write and a set write invalidated their logical cache key with the full
//! `MultiLayerCache::invalidate`. For a key no tier holds -- every ordinary write, since a string's
//! logical key is not filled by `StringGet` -- that call still walks the bookkeeping of every tier
//! (disk index, manifest delete line, pmem delete line, ssd block delete) and removes nothing. It
//! was the largest allocation class of a string write. Hash writes already ask `peek` first
//! (`invalidate_if_cached`); string and set writes now do too.
//!
//! TWO ARMS, BECAUSE THEY GUARD OPPOSITE FAILURES.
//!   * The default-build tests are THE CONTROL: when something IS cached under the key, a write
//!     must still remove it. Skipping the invalidation without that would serve stale bytes.
//!   * `an_uncached_write_does_not_enter_the_invalidation_class` needs `alloc-probe` and
//!     `--ignored`. It reads the per-class ledger, whose `cache_invalidation` row is exactly the
//!     work being skipped: it is zero only on the asking path, and positive for a cached key.
//!
//! rust-internal: measures this crate's own request paths, no product behaviour

#![allow(clippy::all)]
use super::*;

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

#[test]
fn a_cached_string_is_still_invalidated_by_a_write() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_on(dir.path());
    let cache = engine.cache();
    let key = CacheKey::string(1, "k");
    cache.put(key.clone(), b"stale".to_vec()).unwrap();
    assert!(cache.peek(&key), "the planted entry must be resident");
    let before = cache.stats().invalidations;

    run(
        &engine,
        Command::StringSet {
            key: "k".to_string(),
            value: b"new".to_vec(),
        },
    );

    assert!(
        !cache.peek(&key),
        "a write left a cached entry behind: a reader would be served it"
    );
    assert_eq!(cache.stats().invalidations, before + 1);
}

#[test]
fn a_cached_set_is_still_invalidated_by_an_add_and_a_remove() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_on(dir.path());
    let cache = engine.cache();
    let key = CacheKey::set_members(1, "s");
    for (label, command) in [
        (
            "add",
            Command::SetAdd {
                key: "s".to_string(),
                member: b"m".to_vec(),
            },
        ),
        (
            "remove",
            Command::SetRemove {
                key: "s".to_string(),
                member: b"m".to_vec(),
            },
        ),
    ] {
        cache.put(key.clone(), b"stale".to_vec()).unwrap();
        assert!(cache.peek(&key), "{label}: the planted entry must be resident");
        let before = cache.stats().invalidations;
        run(&engine, command);
        assert!(!cache.peek(&key), "{label} left a cached members entry behind");
        assert_eq!(cache.stats().invalidations, before + 1, "{label}");
    }
}

#[test]
fn a_write_to_a_key_nothing_caches_invalidates_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_on(dir.path());
    let cache = engine.cache();
    run(
        &engine,
        Command::StringSet {
            key: "fresh".to_string(),
            value: b"v".to_vec(),
        },
    );
    run(
        &engine,
        Command::SetAdd {
            key: "fresh-set".to_string(),
            member: b"m".to_vec(),
        },
    );
    run(
        &engine,
        Command::SetRemove {
            key: "fresh-set".to_string(),
            member: b"m".to_vec(),
        },
    );
    assert_eq!(
        cache.stats().invalidations,
        0,
        "nothing was cached under these keys, so no invalidation had anything to remove"
    );
}

#[cfg(feature = "alloc-probe")]
mod probe {
    use super::*;
    use crate::alloc_probe::{AllocClass, ClassSpan, Probe};

    const OPS: usize = 200;

    /// Allocations in the `cache_invalidation` class, and in total, per operation.
    fn per_op(cached: bool, make: impl Fn(usize) -> Command) -> (f64, f64) {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = engine_on(dir.path());
        let cache = engine.cache();
        let commands: Vec<Command> = (0..OPS).map(&make).collect();
        if cached {
            for j in 0..OPS {
                let name = format!("o-{j:05}");
                cache
                    .put(CacheKey::string(1, &name), b"stale".to_vec())
                    .unwrap();
                cache
                    .put(CacheKey::set_members(1, &name), b"stale".to_vec())
                    .unwrap();
            }
        }
        let total = Probe::start();
        let span = ClassSpan::open();
        for command in commands {
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command,
            });
            assert!(response.status.ok);
        }
        let classes = span.close().expect("built with `alloc-probe`");
        let row = classes.classes.row(AllocClass::CacheInvalidation).allocs;
        (
            row as f64 / OPS as f64,
            total.stop().allocs as f64 / OPS as f64,
        )
    }

    #[test]
    #[ignore = "measurement: run with --features alloc-probe --ignored"]
    fn an_uncached_write_does_not_enter_the_invalidation_class() {
        let string = |j: usize| Command::StringSet {
            key: format!("o-{j:05}"),
            value: vec![b'v'; 64],
        };

        // THE CONTROL, FIRST. With an entry cached under every key the same writes must charge the
        // class; a probe that read zero there could not see the work it claims was skipped.
        let (string_cached, string_cached_total) = per_op(true, string);
        println!(
            "control  cached key: string {string_cached:.1} class / {string_cached_total:.1} total allocs/op"
        );
        assert!(
            string_cached >= 5.0,
            "the control did not charge the class ({string_cached:.1})"
        );

        let (string_class, string_total) = per_op(false, string);
        println!("uncached key: string {string_class:.1} class / {string_total:.1} total allocs/op");
        assert_eq!(
            string_class, 0.0,
            "a string write to an uncached key still walks the tiers"
        );
    }
}
