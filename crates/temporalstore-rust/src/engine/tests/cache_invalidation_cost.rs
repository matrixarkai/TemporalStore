// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHERE THE FIFTY-FOUR ALLOCATIONS OF ONE CACHE INVALIDATION GO.
//!
//! `alloc_class_scale` measured a value write and found one class larger by CALL COUNT than the
//! whole rest of the write together: `cache_invalidation`, 54.06 allocations for 1,474 bytes,
//! flat per record at 2,000 and at 20,000 and again at 400,000 and 4,000,000. Flat is the
//! important word. This is not a sink that grows with the store; it is a constant paid on every
//! single write, and nobody had asked what it is made of.
//!
//! This module asks. It is a decomposition by DIFFERENCE over the serving cache's public surface,
//! which is the only instrument available: the charge sits in `invalidate_cache_key`, but the work
//! is in a separate crate pinned by revision, so no counter can be placed inside the thing being
//! measured.
//!
//! THE LADDER. Same cache, built exactly as `TemporalEngine::with_local_dirs` builds it, cold, and
//! every key absent from it -- which is the regime of the write path that pays the 54, because a
//! write invalidates a key the store has just written and no reader has yet asked for.
//!
//! ```text
//!   arm                         what it adds over the arm above
//!   key_only                    CacheKey::string, and nothing else
//!   peek                        + the read-side tier probe
//!   invalidate_memory_only      + memory and pmem map removal, pins, access record
//!   invalidate                  + THE PERSISTENCE BOOKKEEPING: disk index, manifest op,
//!                                 pmem delete line, ssd block delete
//! ```
//!
//! WHAT IS NOT IN THE 54, and it matters for reading the table: the class is charged INSIDE
//! `invalidate_cache_key`, whose key argument is built by the caller. `CacheKey::string` is
//! therefore outside it. The quantity to compare against 54.06 is `invalidate` MINUS `key_only`.
//!
//! THE RECONCILIATION IS AGAINST AN INSTRUMENT THIS LADDER DOES NOT FEED. Every figure here comes
//! from `alloc_probe::Probe`, the process-wide counter, over a synthetic loop. The 54.06 it is
//! checked against comes from the per-class ledger, charged inside the primitive, during a real
//! `batch_execute` ingest in `alloc_class_scale`. Two instruments, two workloads, one number; the
//! residual between them is reported and asserted small rather than computed from the rows.
//!
//! THE STORE PATH IS HELD CONSTANT AND IT IS `/tmp/.tmpXXXXXX`, fifteen characters, the same
//! `tempfile::tempdir()` the class table is taken under. #1922 found by accident that what a write
//! allocates MOVES WITH THE LENGTH OF THE STORE'S DIRECTORY PATH -- `cache_invalidation` went
//! 1,474 -> 1,546 -> 1,606 bytes a record across three paths of 15, 27 and 37 characters -- and
//! read it as a one percent debug-against-release difference until the pair was run properly. One
//! test here varies the path DELIBERATELY, to say which part of the cost that effect lives in.

// Everything below except the two-build guard at the end of this file is measurement apparatus,
// and measurement apparatus only exists in a build that installs the counting allocator. Gated
// item by item rather than as a whole module, because one guard here MUST run in a build without
// the feature -- that is the whole point of it.
#[cfg(feature = "alloc-probe")]
use super::*;

#[cfg(feature = "alloc-probe")]
use crate::alloc_probe::{AllocCounts, Probe};
#[cfg(feature = "alloc-probe")]
use matrixcache::{CacheKey, MultiLayerCache};

#[cfg(feature = "alloc-probe")]
const SMALL: usize = 2_000;
#[cfg(feature = "alloc-probe")]
const LARGE: usize = 20_000;
#[cfg(feature = "alloc-probe")]
const MEMORY_CAPACITY: usize = 64 * 1024 * 1024;
#[cfg(feature = "alloc-probe")]
const VALUE_BYTES: usize = 1_024;
#[cfg(feature = "alloc-probe")]
const BATCH: usize = 100;

/// The key shape the ingest that measured the 54 uses: `k-` and eight digits, on shard 1.
///
/// Key LENGTH is an input to this measurement -- `CacheKey::string` copies the key and the
/// manifest encoders format it again -- so a ladder built on keys of a different width would
/// report a different number and reconcile against nothing.
#[cfg(feature = "alloc-probe")]
fn corpus(n: usize) -> Vec<String> {
    (0..n).map(|index| format!("k-{index:08}")).collect()
}

/// Allocations attributable to `work`. The corpus is built BEFORE the span opens.
#[cfg(feature = "alloc-probe")]
fn measure(work: impl FnOnce()) -> AllocCounts {
    let probe = Probe::start();
    work();
    probe.stop()
}

/// A cache built the way the engine builds its serving cache.
#[cfg(feature = "alloc-probe")]
fn engine_cache(dir: &std::path::Path) -> MultiLayerCache {
    MultiLayerCache::new(MEMORY_CAPACITY, dir)
}

/// What the SHIPPED call site charges, per stored record, on a real ingest.
///
/// This is the other instrument. The ladder above drives the cache crate directly and counts with
/// the process-wide span counter; this drives `batch_execute` and reads the per-class ledger that
/// `invalidate_cache_key` charges from inside itself. Neither feeds the other, and the difference
/// between them is the residual -- which is the only reason a residual here means anything.
///
/// It is also what makes this module a guard on the write path rather than a microbenchmark:
/// a change that stops the engine invalidating, or stops it charging what it invalidates, moves
/// this number and not the ladder.
#[cfg(feature = "alloc-probe")]
fn in_situ_charge_per_record(records: usize) -> (f64, f64) {
    use crate::alloc_probe::{AllocClass, ClassSpan};

    let dir = tempfile::tempdir().expect("tempdir");
    let engine = TemporalEngine::with_local_dirs(
        MEMORY_CAPACITY,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    // One write before the span, so a lazily built table is not charged to this class.
    engine.execute(crate::types::ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "warm".to_string(),
            value: vec![7u8; VALUE_BYTES],
        },
    });

    // THE CORPUS IS BUILT BEFORE THE SPAN OPENS, for the reason `alloc_class_scale` gives: a probe
    // that assembles its commands inside the measured window charges the store for its own fixture.
    let keys = corpus(records);
    let mut batches: Vec<Vec<Command>> = Vec::new();
    for chunk in keys.chunks(BATCH) {
        batches.push(
            chunk
                .iter()
                .map(|key| Command::StringSet {
                    key: key.clone(),
                    value: vec![7u8; VALUE_BYTES],
                })
                .collect(),
        );
    }

    let span = ClassSpan::open();
    for commands in batches.drain(..) {
        let response = engine.batch_execute(crate::types::BatchExecuteRequest {
            shard_id: 1,
            commands,
        });
        assert!(response.status.ok, "ingest failed: {:?}", response.status);
    }
    let counts = span
        .close()
        .expect("built with `alloc-probe`, so these are measurements");
    let row = counts.classes.row(AllocClass::CacheInvalidation);
    drop(engine);
    (
        row.allocs as f64 / records as f64,
        row.alloc_bytes as f64 / records as f64,
    )
}

#[cfg(feature = "alloc-probe")]
#[derive(Clone, Copy)]
struct Arm {
    label: &'static str,
    allocs: u64,
    bytes: u64,
}

#[cfg(feature = "alloc-probe")]
impl Arm {
    fn per(&self, n: usize) -> f64 {
        self.allocs as f64 / n as f64
    }
    fn bytes_per(&self, n: usize) -> f64 {
        self.bytes as f64 / n as f64
    }
}

#[cfg(feature = "alloc-probe")]
fn arm(label: &'static str, counts: AllocCounts) -> Arm {
    Arm {
        label,
        allocs: counts.allocs,
        bytes: counts.alloc_bytes,
    }
}

/// Run the whole ladder at one corpus size, under one store path.
#[cfg(feature = "alloc-probe")]
fn ladder(n: usize, dir: &std::path::Path) -> Vec<Arm> {
    let keys = corpus(n);

    // Each arm gets its OWN cache, so an arm never inherits the tier state a previous arm left.
    let key_cache = engine_cache(&dir.join("c-key"));
    let peek_cache = engine_cache(&dir.join("c-peek"));
    let mem_cache = engine_cache(&dir.join("c-mem"));
    let full_cache = engine_cache(&dir.join("c-full"));
    let batch_cache = engine_cache(&dir.join("c-batch"));
    // The SAME cache this engine already builds when a node is configured without a disk cache
    // tier -- `with_local_dirs_block_store_options_and_disk_cache(.., false)`. Not a hypothetical
    // shape: a supported deployment, measured here beside the default one.
    let no_disk_cache = MultiLayerCache::with_tiering_policy(
        dir.join("c-nodisk"),
        matrixcache::CacheTieringPolicy {
            memory_capacity_bytes: MEMORY_CAPACITY,
            pmem_capacity_bytes: 0,
            ssd_capacity_bytes: 0,
            ..Default::default()
        },
        matrixcache::CacheBlockOptions::default(),
    );

    // The shipped call returns a Result and `invalidate_cache_key` discards it. A cache that was
    // never started answers `Err(Stopped)` and returns having done NOTHING, which would read as a
    // very cheap invalidation rather than as an invalidation that did not happen.
    assert!(
        full_cache
            .invalidate(&CacheKey::string(1, "started-probe"))
            .is_ok(),
        "the cache under measurement is not started, so `invalidate` returns early and every \
         figure below is the cost of doing nothing"
    );
    let _ = key_cache.peek(&CacheKey::string(1, "started-probe"));

    let key_only = arm(
        "key_only",
        measure(|| {
            for key in &keys {
                std::hint::black_box(CacheKey::string(1, key));
            }
        }),
    );
    let peek = arm(
        "peek",
        measure(|| {
            for key in &keys {
                std::hint::black_box(peek_cache.peek(&CacheKey::string(1, key)));
            }
        }),
    );
    let memory_only = arm(
        "invalidate_memory_only",
        measure(|| {
            for key in &keys {
                mem_cache.invalidate_memory_only(&CacheKey::string(1, key));
            }
        }),
    );
    let full = arm(
        "invalidate",
        measure(|| {
            for key in &keys {
                let _ = full_cache.invalidate(&CacheKey::string(1, key));
            }
        }),
    );

    // CANDIDATE, PRICED: one call per key replaced by one call per hundred keys. Same key set,
    // same round; only WHEN each key leaves the cache changes.
    let batched = arm(
        "invalidate_batch(100)",
        measure(|| {
            for chunk in keys.chunks(100) {
                let batch = chunk
                    .iter()
                    .map(|key| CacheKey::string(1, key))
                    .collect::<Vec<_>>();
                let _ = batch_cache.invalidate_batch(&batch);
            }
        }),
    );
    // CANDIDATE, PRICED: the disk cache tier switched off, which this engine already supports.
    let no_disk = arm(
        "invalidate@no-disk-tier",
        measure(|| {
            for key in &keys {
                let _ = no_disk_cache.invalidate(&CacheKey::string(1, key));
            }
        }),
    );

    vec![key_only, peek, memory_only, full, batched, no_disk]
}

#[cfg(feature = "alloc-probe")]
fn print_ladder(n: usize, arms: &[Arm]) {
    println!("\n  {n} keys, cache cold, every key absent");
    println!(
        "  {:<24} {:>12} {:>10} {:>14} {:>11}",
        "arm", "allocs", "per key", "alloc bytes", "B per key"
    );
    for entry in arms {
        println!(
            "  {:<24} {:>12} {:>10.3} {:>14} {:>11.1}",
            entry.label,
            entry.allocs,
            entry.per(n),
            entry.bytes,
            entry.bytes_per(n)
        );
    }
}

/// The instrument's own control: plant a known number of allocations and recover exactly that many.
///
/// Without this the ladder cannot tell a correct reading from a counter that is off by a constant
/// per iteration, and every difference it reports would inherit the error.
///
/// THE PROBE IS SPELLED OUT IN FULL BELOW ON PURPOSE. The repo-wide guard
/// `every_counting_allocator_probe_is_gated_on_the_feature_that_installs_it` finds probe-reading
/// tests by matching the fully qualified spelling of the counter type and its start call, so a
/// module that imports those names and uses them bare is INVISIBLE to it. Measured, not assumed:
/// with every read in this file going through the import, that guard scanned this file, found no
/// markers at all, and went on passing after the feature gate on this very test was deleted.
/// Written out below it sees this test, counts it, and fails if the gate is removed.
///
/// The marker must not appear in PROSE either, here or anywhere else. The guard walks BACKWARDS
/// from each match to the nearest enclosing test attribute, and a doc comment sits above its own
/// function's attributes -- so a marker written in a comment is attributed to the PREVIOUS test in
/// the file, or to no test at all. Spelling it in this doc comment turned this guard red, which is
/// how that was learned.
#[cfg(feature = "alloc-probe")]
#[test]
fn the_span_counter_recovers_exactly_the_allocations_planted_in_it() {
    const PLANTED: usize = 512;
    let probe = crate::alloc_probe::Probe::start();
    for index in 0..PLANTED {
        std::hint::black_box(vec![0u8; 64 + index % 8]);
    }
    let counts: crate::alloc_probe::AllocCounts = probe.stop();
    assert_eq!(
        counts.allocs, PLANTED as u64,
        "planted {PLANTED} allocations and the span counter recovered {}; every difference the \
         ladder reports is taken with this instrument",
        counts.allocs
    );
}

/// Without the counting allocator every arm reads zero, which is indistinguishable from an
/// invalidation that allocates nothing. Make not-counting representable rather than silent.
#[test]
fn the_invalidation_ladder_refuses_to_report_without_the_counting_allocator() {
    let counted = crate::alloc_probe::counted_now();
    #[cfg(feature = "alloc-probe")]
    assert!(
        counted.is_some(),
        "built with `alloc-probe` and the counters are still absent"
    );
    #[cfg(not(feature = "alloc-probe"))]
    assert!(
        counted.is_none(),
        "built without `alloc-probe`, so a reading here would be a table of zeros presented as a \
         measurement"
    );
}

#[cfg(feature = "alloc-probe")]
#[test]
#[ignore = "measurement; run with --features alloc-probe --ignored --nocapture"]
fn where_the_fifty_four_allocations_of_one_cache_invalidation_go() {
    let dir = tempfile::tempdir().expect("tempdir");
    println!(
        "\nSTORE PATH HELD CONSTANT: {} ({} characters)",
        dir.path().display(),
        dir.path().as_os_str().len()
    );

    let small = ladder(SMALL, &dir.path().join("small"));
    let large = ladder(LARGE, &dir.path().join("large"));
    print_ladder(SMALL, &small);
    print_ladder(LARGE, &large);

    let find = |arms: &[Arm], label: &str| -> Arm {
        *arms
            .iter()
            .find(|entry| entry.label == label)
            .expect("arm present")
    };

    println!("\n  THE BREAKDOWN, per key, at {LARGE} keys");
    println!("  {:<44} {:>10} {:>11}", "term", "allocs", "bytes");
    let key_only = find(&large, "key_only");
    let peek = find(&large, "peek");
    let memory_only = find(&large, "invalidate_memory_only");
    let full = find(&large, "invalidate");
    let rows: Vec<(&str, f64, f64)> = vec![
        (
            "CacheKey::string (OUTSIDE the charged class)",
            key_only.per(LARGE),
            key_only.bytes_per(LARGE),
        ),
        (
            "memory+pmem removal, pins, access record",
            memory_only.per(LARGE) - key_only.per(LARGE),
            memory_only.bytes_per(LARGE) - key_only.bytes_per(LARGE),
        ),
        (
            "persistence bookkeeping (disk/ssd/pmem)",
            full.per(LARGE) - memory_only.per(LARGE),
            full.bytes_per(LARGE) - memory_only.bytes_per(LARGE),
        ),
    ];
    for (label, allocs, bytes) in &rows {
        println!("  {label:<44} {allocs:>10.3} {bytes:>11.1}");
    }
    let charged = full.per(LARGE) - key_only.per(LARGE);
    println!(
        "  {:<44} {:>10.3} {:>11.1}",
        "= what the charged class pays (invalidate)",
        charged,
        full.bytes_per(LARGE) - key_only.bytes_per(LARGE)
    );
    println!(
        "  {:<44} {:>10.3}",
        "(read-side floor, for scale: peek)",
        peek.per(LARGE) - key_only.per(LARGE)
    );

    // FLATNESS. Every arm is a per-key cost or the ladder is measuring its own fixture.
    for (small_arm, large_arm) in small.iter().zip(large.iter()) {
        assert_eq!(small_arm.label, large_arm.label);
        let ratio = large_arm.per(LARGE) / small_arm.per(SMALL);
        assert!(
            ratio > 0.5 && ratio < 2.0,
            "{} costs {:.3} allocations a key at {SMALL} and {:.3} at {LARGE} ({ratio:.3}x); a \
             ladder rung that is not flat is measuring the loop, not the call",
            small_arm.label,
            small_arm.per(SMALL),
            large_arm.per(LARGE)
        );
    }

    // NON-VACUITY: a rung that costs nothing is a rung that did not run.
    for entry in &large {
        assert!(
            entry.allocs > 0,
            "{} allocated nothing across {LARGE} keys",
            entry.label
        );
    }

    // THE FINDING, AS AN ASSERTION. Dropping the key from the memory and pmem maps, clearing its
    // pin entry and emitting the access record costs NOTHING beyond building the key: the whole
    // 54 is the persistence bookkeeping underneath it. If a later revision of the cache changes
    // that, this is where it is noticed rather than in a table nobody re-reads.
    assert!(
        memory_only.per(LARGE) - key_only.per(LARGE) < 1.0,
        "the memory-tier half of an invalidation now costs {:.3} allocations a key on top of the \
         key itself; it was free when this was measured, and the claim that the whole cost is \
         persistence bookkeeping rests on it being free",
        memory_only.per(LARGE) - key_only.per(LARGE)
    );
    assert!(
        full.per(LARGE) - memory_only.per(LARGE) > 40.0,
        "the persistence term is {:.3} allocations a key, not the ~54 that made this the largest \
         class by count on the write path; either the cache revision changed or this ladder is no \
         longer measuring the shipped call",
        full.per(LARGE) - memory_only.per(LARGE)
    );

    // THE CANDIDATES, PRICED SEPARATELY. Neither is applied; both are measured so the reason for
    // declining them carries a number.
    let batched = find(&large, "invalidate_batch(100)");
    let no_disk = find(&large, "invalidate@no-disk-tier");
    println!("\n  CANDIDATES, priced against {:.3} allocations a key", full.per(LARGE));
    println!(
        "  {:<44} {:>10.3} {:>11.3}",
        "one call per 100 keys instead of per key",
        batched.per(LARGE),
        full.per(LARGE) - batched.per(LARGE)
    );
    println!(
        "  {:<44} {:>10.3} {:>11.3}",
        "disk cache tier off (already supported)",
        no_disk.per(LARGE),
        full.per(LARGE) - no_disk.per(LARGE)
    );
    assert!(
        batched.allocs > 0 && no_disk.allocs > 0,
        "a priced candidate that allocated nothing did not run"
    );

    // THE RECONCILIATION, against an instrument this ladder does not feed, MEASURED IN THIS RUN
    // rather than quoted from the pull request that found it. Taken at two corpus sizes so a fixed
    // per-ingest cost divides away instead of hiding in a single number.
    let (in_situ_small, in_situ_small_bytes) = in_situ_charge_per_record(SMALL);
    let (in_situ_large, in_situ_large_bytes) = in_situ_charge_per_record(LARGE);
    println!("\n  RECONCILIATION (two instruments, two workloads)");
    println!(
        "  {:<44} {:>10.3} {:>11.1}",
        "in-situ class ledger, per record, 2,000", in_situ_small, in_situ_small_bytes
    );
    println!(
        "  {:<44} {:>10.3} {:>11.1}",
        "in-situ class ledger, per record, 20,000", in_situ_large, in_situ_large_bytes
    );
    println!(
        "  {:<44} {:>10.3} {:>11.1}",
        "this ladder, invalidate minus key_only",
        charged,
        full.bytes_per(LARGE) - key_only.bytes_per(LARGE)
    );
    let residual = in_situ_large - charged;
    println!(
        "  {:<44} {:>10.3}",
        "residual (independent, not a row sum)", residual
    );

    // The in-situ charge must be a real reading, not a class nothing entered.
    assert!(
        in_situ_large > 0.0,
        "the `cache_invalidation` class recorded nothing across {LARGE} records; either the write \
         path stopped invalidating or the charge stopped being made, and both read as a free \
         invalidation here"
    );
    // FLAT PER RECORD, which is the claim this whole subject rests on: a constant paid on every
    // write, not a cost that grows with the store.
    let in_situ_ratio = in_situ_large / in_situ_small;
    assert!(
        in_situ_ratio > 0.5 && in_situ_ratio < 2.0,
        "the shipped call site charged {in_situ_small:.3} allocations a record at {SMALL} and \
         {in_situ_large:.3} at {LARGE} ({in_situ_ratio:.3}x); this was FLAT when it was measured, \
         and a per-write constant and a sink that grows with the store want opposite fixes"
    );
    assert!(
        residual.abs() < 6.0,
        "the ladder says one invalidation costs {charged:.3} allocations and the class ledger \
         charged inside the shipped call site says {in_situ_large:.3}; a residual of \
         {residual:.3} means the synthetic loop is not in the same regime as the write path and \
         the breakdown above it describes something else"
    );
}

/// WHICH PART OF THE COST MOVES WITH THE STORE'S DIRECTORY NAME.
///
/// #1922 recorded, without chasing it, that `cache_invalidation`'s BYTES per record went
/// 1,474 -> 1,546 -> 1,606 across store paths of 15, 27 and 37 characters while its allocation
/// COUNT did not move at all. This says where that lives: the same invalidation, the same keys,
/// two cache directories differing only in name length.
#[cfg(feature = "alloc-probe")]
#[test]
#[ignore = "measurement; run with --features alloc-probe --ignored --nocapture"]
fn the_bytes_an_invalidation_allocates_move_with_the_cache_directory_name() {
    const N: usize = 5_000;
    let dir = tempfile::tempdir().expect("tempdir");
    let keys = corpus(N);

    let short = dir.path().join("s");
    let long = dir.path().join("l".repeat(65));
    let extra = long.as_os_str().len() - short.as_os_str().len();

    let short_cache = engine_cache(&short);
    let long_cache = engine_cache(&long);

    let short_counts = measure(|| {
        for key in &keys {
            let _ = short_cache.invalidate(&CacheKey::string(1, key));
        }
    });
    let long_counts = measure(|| {
        for key in &keys {
            let _ = long_cache.invalidate(&CacheKey::string(1, key));
        }
    });

    let short_allocs = short_counts.allocs as f64 / N as f64;
    let long_allocs = long_counts.allocs as f64 / N as f64;
    let short_bytes = short_counts.alloc_bytes as f64 / N as f64;
    let long_bytes = long_counts.alloc_bytes as f64 / N as f64;
    println!(
        "\n  cache dir {extra} chars longer, {N} invalidations each\n  \
         {:<24} {:>10} {:>12}\n  {:<24} {:>10.3} {:>12.1}\n  {:<24} {:>10.3} {:>12.1}\n  \
         {:<24} {:>10.3} {:>12.1}",
        "arm",
        "allocs/key",
        "bytes/key",
        "short path",
        short_allocs,
        short_bytes,
        "long path",
        long_allocs,
        long_bytes,
        "delta",
        long_allocs - short_allocs,
        long_bytes - short_bytes,
    );

    assert!(
        short_counts.allocs > 0 && long_counts.allocs > 0,
        "nothing was counted, so neither column is a measurement"
    );
    // THE COUNT DOES NOT MOVE. This is the half of #1922's observation that says the effect is
    // capacity, not extra calls -- the same allocations, asked for more bytes.
    assert!(
        (long_allocs - short_allocs).abs() < 1.0,
        "the allocation COUNT moved {short_allocs:.3} -> {long_allocs:.3} with nothing but the \
         directory name; #1922 recorded the count as unchanged and only the bytes moving"
    );
    // AND THE BYTES DO. Asserted as a floor rather than a value: how much depends on how the
    // allocator rounds a growing path buffer, and that is not this repository's to pin.
    assert!(
        long_bytes > short_bytes,
        "a cache directory {extra} characters longer allocated {long_bytes:.1} bytes a key \
         against {short_bytes:.1}; #1922's observation that the byte cost follows the store path \
         does not reproduce, so the note it left should be corrected rather than repeated"
    );
}
