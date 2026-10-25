// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHETHER THE STORAGE-EVICTION STAGE IS SAFE TO SHIP ON, and at what threshold.
//!
//! #2058 and #2059 measured what a dump-then-release RECOVERS on the shipped 0..1023 range: about
//! half the per-key resident cost. The exact share is not quoted here because its denominator moved
//! when #2060 narrowed the block address, and a figure copied across that boundary is stale by a few
//! bytes a key -- #2059 is the fixture to read it from.
//!
//! TWO THINGS THAT MEASUREMENT DOES NOT SAY, and that a default decision needs. First, it is a
//! MANY-ROUNDS steady state, not what one cycle does: `eviction_batch_limit` is 16 and
//! `eviction_count_limit` is 100, so one round considers 16 buckets and takes at most 100. The
//! headline is what the stage CONVERGES on, not what a cycle recovers. Second, both measured the
//! store at REST -- seed, release, read back. That is the right way to measure a recovery and the
//! wrong way to decide a default, because the release does not run at rest on a live node: it runs
//! on a shard that is serving.
//!
//! `apply_storage_eviction` takes `self.shards.write()` and calls `release_bucket_blocks` inside
//! that guard (`storage_lifecycle_methods.rs`), and the release asks the model maps what a reload
//! would rebuild -- a pass over every live address in the shard. So switching the stage on by
//! default puts a whole-store walk under the shard write guard on every maintenance round, and
//! every serving read and write on that shard queues behind it. That is the cost of the change.
//! What it must not do is answer a read WRONGLY, and nothing in the tree checked that: the
//! existing at-rest fixtures invalidate the cache and then read, single-threaded, after the round
//! has returned.
//!
//! `reads_stay_correct_while_the_eviction_cycle_runs` is that missing gate. Readers run against
//! the shard WHILE rounds release buckets under them, every value is a function of its key so a
//! read resolved through the wrong block fails rather than passing on a lucky length, and the
//! mismatch list is asserted empty rather than printed.

#![allow(clippy::all)]
use super::*;

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;

/// The shipped routing range since #1973, and the one `docs/runtime_tuning.md` tells an operator to
/// set. On the bare `load_shard` default every key gets a bucket of its own, which is the LEGACY
/// range: releasing there frees a per-key node and tells you nothing about a shard where keys share
/// one.
const SHIPPED_END_BUCKET: u32 = 1023;

/// A value that is a function of its key, so a read that resolved through the WRONG block fails
/// here instead of passing because the length happened to match.
fn keyed_value(index: usize) -> Vec<u8> {
    let mut value = format!("{index:06}:").into_bytes();
    value.resize(VALUE_LEN, b'v');
    value[VALUE_LEN - 1] = (index % 251) as u8;
    value
}

const VALUE_LEN: usize = 96;

fn key_of(index: usize) -> String {
    format!("evictlive-{index:06}")
}

/// A shard loaded on the SHIPPED range and seeded with `keys` string records.
fn seeded_runtime(dir: &std::path::Path, keys: usize) -> DataNodeRuntime {
    let engine = TemporalEngine::with_local_dirs(
        16 * 1024 * 1024,
        dir.join("cache"),
        dir.join("pages"),
        dir.join("indexes"),
    );
    let response = engine.load_shard_with(crate::control::LoadShardRequest {
        shard_id: 1,
        table_name: "evict-live".to_string(),
        shard_uri: "local://evict-live/1".to_string(),
        start_routing_bucket: 0,
        end_routing_bucket: SHIPPED_END_BUCKET,
        readonly: false,
        load_version: 1,
        local_node_id: Some(1),
    });
    assert!(
        response.status.ok,
        "the fixture could not load shard 1 on 0..{SHIPPED_END_BUCKET}: {:?}",
        response.status
    );
    for index in 0..keys {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: key_of(index),
                value: keyed_value(index),
            },
        });
        assert!(response.status.ok, "write {index}: {:?}", response.status);
    }
    DataNodeRuntime::new_without_workers_with_options(
        engine,
        DataNodeRuntimeOptions {
            worker_threads: 0,
            max_queue_depth: 64,
            max_background_queue_depth: 8,
        },
    )
}

/// Read every seeded key back, so the record cache holds something.
///
/// WHY THIS EXISTS AND WHY IT IS NOT OPTIONAL. The first version of the threshold measurements below
/// read the cache terms straight off a freshly seeded store and got ZERO for both -- the write path
/// does not populate the record cache, only the read path does. A "warm" arm that is really a
/// write-only arm reports `cache_memory_bytes = 0` and makes any claim about what the cache term can
/// reach vacuous. Every measurement here that names the cache asserts the warming worked.
fn warm_cache_by_reading(engine: &TemporalEngine, shard_id: ShardId, prefix: &str, keys: usize) {
    for index in 0..keys {
        let response = engine.execute(ExecuteRequest {
            shard_id,
            command: Command::StringGet {
                key: format!("{prefix}{index:06}"),
            },
        });
        assert!(
            response.status.ok,
            "warming read {prefix}{index:06}: {:?}",
            response.status
        );
    }
}

/// The configuration a default-on change would ship, so the gates below run that rather than a
/// hand-tuned neighbour of it.
///
/// NOT the shipped default. `enable_evict` and `eviction_dump_before_evict` have since become the
/// shipped values, but the threshold here is 1 so the fixtures' own pressure clears it, and the
/// shipped bound is pinned AWAY so that it does not.
///
/// WHY THE BOUND IS PINNED TO 0 HERE AND AT EVERY OTHER FIXTURE IN THIS FILE THAT FORCES THE GATE.
/// The shipped default is now `index_resident_limit_bytes` = 64 MiB, and the periodic loop takes
/// the HIGHER of the bound and the threshold. A fixture holding a few thousand keys carries a few
/// hundred kilobytes of resident index, so at the shipped bound every one of these fixtures would
/// decline with `index_resident_below_bound` -- and the measurements about the WIDE arm would
/// silently become measurements of a refusal. Pinning 0 keeps each fixture testing what it was
/// written to test. The shipped pair has its own gates; see
/// `the_shipped_default_opens_the_bound_and_never_the_wide_gate`.
fn proposed_options() -> StorageManagerOptions {
    StorageManagerOptions {
        enable_evict: true,
        eviction_dump_before_evict: true,
        eviction_memory_pressure_threshold: 1,
        index_resident_limit_bytes: 0,
        ..StorageManagerOptions::default()
    }
}

/// THE GATE ON THE WHOLE CHANGE: a read answered WHILE buckets are being released is still correct.
///
/// Every existing release fixture is at rest. This one serves reads from four threads while the
/// maintenance round releases buckets under them, and the cache is invalidated between rounds so a
/// reader is forced down the COLD path -- through `bucket_index_block_address`, which is the lookup
/// a released bucket cannot answer from its own block list and has to answer from the model maps.
/// That is the path a release changes, so it is the path a concurrent reader has to be checked on.
///
/// WHAT WOULD MAKE THIS FAIL RATHER THAN BE VACUOUS, all asserted:
///   * reads actually happened (`total_reads > 0`),
///   * buckets were actually released DURING the run, not before or after it,
///   * the resident index actually fell, so the rounds did the thing whose safety is in question.
/// Without those three a run where the stage never fired, or where the readers never started,
/// would pass with an empty mismatch list and prove nothing.
///
/// A WRONG ANSWER IS A FAILURE, NOT A WARNING. The mismatch list is asserted empty and reports the
/// first few; a panic inside a reader also fails the test, because the join is unwrapped.
#[test]
fn reads_stay_correct_while_the_eviction_cycle_runs() {
    const KEYS: usize = 4_000;
    const READER_THREADS: usize = 4;
    const ROUNDS: usize = 8;

    let dir = tempdir().unwrap();
    let runtime = seeded_runtime(dir.path(), KEYS);
    let engine = runtime.engine();

    // DENOMINATORS, before anything is released.
    let index_before = engine.bucket_index_resident_bytes(1);
    assert!(
        index_before > 0,
        "the fixture left no resident index, so there is nothing for a round to release"
    );
    assert!(
        engine.released_bucket_index_buckets(1).is_empty(),
        "the fixture started with buckets already released, so a release below proves nothing"
    );

    let stop = Arc::new(AtomicBool::new(false));
    let reads = Arc::new(AtomicUsize::new(0));
    // Bounded: a reader that found everything wrong should not grow this without limit.
    let mismatches: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));

    let mut readers = Vec::new();
    for offset in 0..READER_THREADS {
        let runtime = runtime.clone();
        let stop = Arc::clone(&stop);
        let reads = Arc::clone(&reads);
        let mismatches = Arc::clone(&mismatches);
        readers.push(thread::spawn(move || {
            let mut index = offset;
            while !stop.load(Ordering::Relaxed) {
                let which = index % KEYS;
                let key = key_of(which);
                let answer = runtime
                    .execute(ExecuteRequest {
                        shard_id: 1,
                        command: Command::StringGet { key: key.clone() },
                    })
                    .response;
                let got = match answer {
                    CommandResponse::Bytes { value } => value,
                    other => {
                        let mut list = mismatches.lock().expect("mismatch lock");
                        if list.len() < 64 {
                            list.push(format!("{key}: a string read answered {other:?}"));
                        }
                        None
                    }
                };
                if got.as_deref() != Some(keyed_value(which).as_slice()) {
                    let mut list = mismatches.lock().expect("mismatch lock");
                    if list.len() < 64 {
                        list.push(format!(
                            "{key}: read back {:?} bytes, expected {VALUE_LEN}",
                            got.as_ref().map(|value| value.len())
                        ));
                    }
                }
                reads.fetch_add(1, Ordering::Relaxed);
                index += READER_THREADS;
            }
        }));
    }

    // The rounds, running against the live readers above.
    let options = proposed_options();
    let mut released_during_run = 0usize;
    let mut refused_during_run = 0usize;
    for round in 0..ROUNDS {
        let report = runtime.run_storage_manager_once(1, options.clone());
        assert!(
            report.executed_stages.iter().any(|stage| stage == "evict"),
            "round {round}: the evict stage did not run, so no read was served against a release: \
             executed={:?} skipped={:?}",
            report.executed_stages,
            report.skipped_stages
        );
        if let Some(eviction) = report.eviction.as_ref() {
            released_during_run += eviction.bucket_index_buckets_released;
            refused_during_run += eviction.bucket_index_release_refused;
        }
        // Force the readers down the COLD path, which is the one a release changes.
        let _ = engine.cache().invalidate_shard(1);
    }

    stop.store(true, Ordering::Relaxed);
    for reader in readers {
        reader.join().expect("a reader thread panicked");
    }

    // --- PROOF THE TREATMENT RAN, before reading the verdict out of it. ---
    let total_reads = reads.load(Ordering::Relaxed);
    assert!(
        total_reads > 0,
        "no concurrent read was issued, so an empty mismatch list means nothing"
    );
    assert!(
        released_during_run > 0,
        "the {ROUNDS} rounds released no bucket while the readers ran, so no read was answered \
         through a released bucket: refused={refused_during_run}"
    );
    let index_after = engine.bucket_index_resident_bytes(1);
    assert!(
        index_after < index_before,
        "the rounds freed no index memory ({index_before} -> {index_after}), so this is not the \
         release whose concurrent safety is in question"
    );

    // --- THE VERDICT. ---
    let list = mismatches.lock().expect("mismatch lock");
    assert!(
        list.is_empty(),
        "{} of {total_reads} concurrent reads were WRONG while buckets were being released \
         ({released_during_run} released): first few {:?}",
        list.len(),
        &list[..list.len().min(5)]
    );

    eprintln!(
        "  [evict-under-load] {total_reads} concurrent reads over {ROUNDS} rounds, \
{READER_THREADS} readers: {released_during_run} buckets released, {refused_during_run} refused, \
resident index {index_before} -> {index_after}, 0 wrong answers"
    );
}

/// WHAT THE THRESHOLD HAS TO BE, measured at three corpus sizes rather than chosen as a round number.
///
/// The gate `apply_storage_eviction` applies is `pressure_before < threshold -> return`. The shipped
/// default is 0, and no `u64` is below 0, so the gate cannot close: the stage fires on every round of
/// every shard regardless of pressure. The neighbouring pipeline's `default_storage_manager_eviction_
/// threshold()` is 1, which is the same thing one byte further along -- any shard that has ever held
/// a record clears it. So there is no considered value anywhere in the tree to adopt, and this
/// measures one.
///
/// WHAT A THRESHOLD IS TRADING. Firing costs a whole-store model-map walk under `shards.write()`;
/// relieving is capped at `eviction_count_limit` (100) buckets per stage. So the threshold should sit
/// above the pressure of a shard too small for that trade to pay, and below the pressure of one where
/// it does. This prints pressure against corpus size so the value is read off a curve rather than
/// asserted, and it prints the per-key slope, which is what makes the number transferable to a corpus
/// nobody measured.
///
/// rust-internal: measures this crate's own resident structures, no product behaviour
#[test]
fn what_pressure_a_shard_carries_at_three_corpus_sizes() {
    // THREE values, not one: a single size cannot show a slope, and a threshold read off one point
    // is a round number with a measurement stapled to it.
    const SIZES: [usize; 3] = [2_000, 8_000, 20_000];

    let mut rows: Vec<(usize, u64, u64, f64)> = Vec::new();
    for keys in SIZES {
        let dir = tempdir().unwrap();
        let runtime = seeded_runtime(dir.path(), keys);
        let engine = runtime.engine();
        // Drop the cache first, so the pressure below is not cache bytes wearing an index label.
        let _ = engine.cache().invalidate_shard(1);

        let options = proposed_options();
        let (pressure, _) = runtime.storage_manager_pressure_snapshot(1, &options);
        let index_resident = engine.bucket_index_resident_bytes(1);
        let total = pressure.eviction_memory_pressure_bytes;
        assert!(
            total > 0,
            "{keys} keys produced no pressure at all, so the signal is not reading this shard"
        );
        assert!(
            index_resident > 0,
            "{keys} keys produced no resident index, so the index term is not in the signal"
        );
        assert!(
            total >= index_resident,
            "the eviction pressure ({total}) does not contain its index term ({index_resident})"
        );
        rows.push((keys, total, index_resident, total as f64 / keys as f64));
    }

    eprintln!("  [evict-threshold] pressure on the shipped 0..{SHIPPED_END_BUCKET} range");
    eprintln!("    keys   pressure_bytes   index_resident   B/key");
    for (keys, total, index_resident, per_key) in &rows {
        eprintln!("    {keys:>6}   {total:>14}   {index_resident:>14}   {per_key:>6.1}");
    }

    // THE SHAPE, asserted: pressure must GROW with the corpus, or a threshold on it cannot
    // distinguish a loaded shard from an empty one and the whole gate is miscalibrated.
    assert!(
        rows[0].1 < rows[1].1 && rows[1].1 < rows[2].1,
        "pressure did not grow with the corpus: {:?} -- a threshold on this signal cannot \
         separate a shard under pressure from one at rest",
        rows.iter().map(|row| (row.0, row.1)).collect::<Vec<_>>()
    );
}

/// WHAT THE DUMP COSTS, AND WHICH HALF OF DUMP-THEN-RELEASE RECOVERS THE INDEX.
///
/// The recovery figure for a default-on change was measured with `dump_before_evict` TRUE, and a
/// release without a dump recovers nothing: a release requires every block clean and undeleted, and a
/// freshly written store is entirely dirty, so every candidate is refused on the dirty term alone.
/// That makes the change dump-then-release rather than release, and it means the recovery is only a
/// win NET OF THE DUMP -- which writes a manifest and cleans every dirty block it touches.
///
/// THREE ARMS, each on its own freshly seeded store so no arm inherits another's cleaning:
///
///   A  the shipped default -- evict OFF. What the REST of the round does to the resident index, so
///      the other two arms are read against a baseline rather than against zero.
///   B  evict ON, dump OFF. The arm that should recover NOTHING.
///   C  evict ON, dump ON. The proposed configuration.
///
/// Without arm A, arm C's drop is credited entirely to eviction when part of it may belong to a
/// stage that was already running. Without arm B, "dump-then-release recovers X" cannot be
/// distinguished from "release recovers X".
///
/// rust-internal: measures this crate's own resident structures, no product behaviour
#[test]
fn what_the_dump_costs_and_which_half_recovers_the_index() {
    const KEYS: usize = 4_000;

    struct Arm {
        label: &'static str,
        index_before: u64,
        index_after: u64,
        released: usize,
        refused: usize,
        manifests: usize,
        stage_before: u64,
        stage_after: u64,
        round_ms: u64,
    }

    fn run_arm(label: &'static str, options: StorageManagerOptions) -> Arm {
        let dir = tempdir().unwrap();
        let runtime = seeded_runtime(dir.path(), KEYS);
        let engine = runtime.engine();
        let _ = engine.cache().invalidate_shard(1);

        let index_before = engine.bucket_index_resident_bytes(1);
        assert!(index_before > 0, "{label}: no resident index to act on");

        // Timed here rather than read from the report: the loop report carries no duration, and the
        // wall clock around the whole round is what a serving request actually queues behind --
        // `apply_storage_eviction` holds `shards.write()` across the release.
        let clock = std::time::Instant::now();
        let report = runtime.run_storage_manager_once(1, options);
        let round_ms = clock.elapsed().as_millis() as u64;
        let index_after = engine.bucket_index_resident_bytes(1);

        // The stage's OWN before/after, which brackets the dump and the release together and so
        // separates what the evict stage did from what the rest of the round did.
        let (stage_before, stage_after) = report
            .eviction
            .as_ref()
            .map(|eviction| (eviction.bucket_index_bytes_before, eviction.bucket_index_bytes_after))
            .unwrap_or((0, 0));
        let (released, refused, manifests) = report
            .eviction
            .as_ref()
            .map(|eviction| {
                (
                    eviction.bucket_index_buckets_released,
                    eviction.bucket_index_release_refused,
                    eviction.dump_manifest_ids.len(),
                )
            })
            .unwrap_or((0, 0, 0));

        Arm {
            label,
            index_before,
            index_after,
            released,
            refused,
            manifests,
            stage_before,
            stage_after,
            round_ms,
        }
    }

    let arm_a = run_arm("A shipped default (evict off)", StorageManagerOptions::default());
    let arm_b = run_arm(
        "B evict on, dump OFF",
        StorageManagerOptions {
            enable_evict: true,
            eviction_dump_before_evict: false,
            eviction_memory_pressure_threshold: 1,
            // PINNED to 0; see `proposed_options`.
            index_resident_limit_bytes: 0,
            ..StorageManagerOptions::default()
        },
    );
    let arm_c = run_arm("C evict on, dump ON (proposed)", proposed_options());

    eprintln!("  [evict-attribution] {KEYS} keys on the shipped 0..{SHIPPED_END_BUCKET} range");
    eprintln!(
        "    {:<32} {:>10} {:>10} {:>9} {:>8} {:>8} {:>9} {:>11} {:>8}",
        "arm", "idx_before", "idx_after", "recovered", "released", "refused", "manifests", "stage_recov", "round_ms"
    );
    for arm in [&arm_a, &arm_b, &arm_c] {
        eprintln!(
            "    {:<32} {:>10} {:>10} {:>9} {:>8} {:>8} {:>9} {:>11} {:>8}",
            arm.label,
            arm.index_before,
            arm.index_after,
            arm.index_before.saturating_sub(arm.index_after),
            arm.released,
            arm.refused,
            arm.manifests,
            arm.stage_before.saturating_sub(arm.stage_after),
            arm.round_ms,
        );
    }

    let recovered = |arm: &Arm| arm.index_before.saturating_sub(arm.index_after);
    eprintln!(
        "    => dump-then-release recovers {} B over the shipped round's own {} B; \
release-without-dump recovers {} B ({} of {} candidates refused)",
        recovered(&arm_c),
        recovered(&arm_a),
        recovered(&arm_b),
        arm_b.refused,
        arm_b.refused + arm_b.released,
    );

    // THE FINDING THIS ARM EXISTS FOR: without the dump, the release gives nothing back.
    assert_eq!(
        arm_b.released, 0,
        "arm B released {} buckets without a dump, which contradicts the dirty precondition -- \
         if this now passes, the precondition changed and the whole case for defaulting \
         dump_before_evict on has to be re-argued",
        arm_b.released
    );
    assert!(
        arm_b.refused > 0,
        "arm B refused nothing and released nothing, so the stage did not reach the release path \
         at all and this arm measures nothing"
    );

    // AND THE ONE THAT MAKES THE CHANGE WORTH MAKING.
    assert!(
        arm_c.released > 0,
        "arm C released nothing, so the proposed configuration does not recover index memory: \
         refused={}",
        arm_c.refused
    );
    assert!(
        recovered(&arm_c) > recovered(&arm_b),
        "dump-then-release ({} B) did not recover more than release alone ({} B)",
        recovered(&arm_c),
        recovered(&arm_b)
    );
}

/// THE ORDERING RACE, WHICH UNDER LOAD IS THE NORMAL CASE: a write that lands while the cycle runs.
///
/// Dump-then-release is two steps, and a write arriving between them re-dirties the bucket. The
/// expected behaviour is that the release then REFUSES that bucket, so nothing is lost -- but
/// "expected" is not a measurement, and a store that drops a write here would look exactly like one
/// that merely released a bucket. So this writes while the rounds run and then checks every key
/// against the LAST version its writer recorded, which is the only check that can see a lost write.
///
/// Each writer owns a disjoint key range, so the final value of a key has exactly one writer and the
/// expected value is known without coordinating between threads. Readers run alongside and check
/// that whatever they see belongs to the key they asked for -- a read resolved through the wrong
/// block fails there rather than at the end.
#[test]
fn a_write_landing_during_the_cycle_is_never_lost() {
    const KEYS: usize = 2_400;
    const WRITERS: usize = 3;
    const VERSIONS: usize = 6;
    const ROUNDS: usize = 8;

    /// Value for `index` at `version`: carries BOTH, so a read that resolved through the wrong
    /// block, or returned a stale version where a newer one was acknowledged, is visible.
    fn versioned_value(index: usize, version: usize) -> Vec<u8> {
        let mut value = format!("{index:06}:{version:04}:").into_bytes();
        value.resize(VALUE_LEN, b'w');
        value
    }

    let dir = tempdir().unwrap();
    let runtime = seeded_runtime(dir.path(), KEYS);
    let engine = runtime.engine();
    let index_before = engine.bucket_index_resident_bytes(1);
    assert!(index_before > 0, "no resident index to act on");

    let stop = Arc::new(AtomicBool::new(false));
    let writes_ok = Arc::new(AtomicUsize::new(0));
    let reads_done = Arc::new(AtomicUsize::new(0));
    let problems: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));

    // Each writer owns [lo, hi) and walks versions 1..=VERSIONS over its range.
    let per_writer = KEYS / WRITERS;
    let mut writers = Vec::new();
    for w in 0..WRITERS {
        let runtime = runtime.clone();
        let writes_ok = Arc::clone(&writes_ok);
        let problems = Arc::clone(&problems);
        let lo = w * per_writer;
        let hi = lo + per_writer;
        writers.push(thread::spawn(move || {
            for version in 1..=VERSIONS {
                for index in lo..hi {
                    let response = runtime.execute(ExecuteRequest {
                        shard_id: 1,
                        command: Command::StringSet {
                            key: key_of(index),
                            value: versioned_value(index, version),
                        },
                    });
                    if response.status.ok {
                        writes_ok.fetch_add(1, Ordering::Relaxed);
                    } else {
                        let mut list = problems.lock().expect("problem lock");
                        if list.len() < 64 {
                            list.push(format!(
                                "write {index}@v{version} was refused: {:?}",
                                response.status
                            ));
                        }
                    }
                }
            }
        }));
    }

    // Readers, checking that a value belongs to the key it was asked for.
    let mut readers = Vec::new();
    for offset in 0..2 {
        let runtime = runtime.clone();
        let stop = Arc::clone(&stop);
        let reads_done = Arc::clone(&reads_done);
        let problems = Arc::clone(&problems);
        readers.push(thread::spawn(move || {
            let mut index = offset;
            while !stop.load(Ordering::Relaxed) {
                let which = index % KEYS;
                let key = key_of(which);
                if let CommandResponse::Bytes { value: Some(value) } = runtime
                    .execute(ExecuteRequest {
                        shard_id: 1,
                        command: Command::StringGet { key: key.clone() },
                    })
                    .response
                {
                    // The key's own ordinal is the first six bytes of its value, whatever the
                    // version. A mismatch is a read that resolved through another key's block.
                    let expected_prefix = format!("{which:06}:").into_bytes();
                    if value.len() != VALUE_LEN || !value.starts_with(&expected_prefix) {
                        let mut list = problems.lock().expect("problem lock");
                        if list.len() < 64 {
                            list.push(format!(
                                "{key}: read a value whose first bytes are {:?}",
                                String::from_utf8_lossy(&value[..value.len().min(12)]).to_string()
                            ));
                        }
                    }
                }
                reads_done.fetch_add(1, Ordering::Relaxed);
                index += 2;
            }
        }));
    }

    // The rounds, running against live writers and readers.
    let options = proposed_options();
    let mut released = 0usize;
    let mut refused = 0usize;
    for _ in 0..ROUNDS {
        let report = runtime.run_storage_manager_once(1, options.clone());
        if let Some(eviction) = report.eviction.as_ref() {
            released += eviction.bucket_index_buckets_released;
            refused += eviction.bucket_index_release_refused;
        }
        let _ = engine.cache().invalidate_shard(1);
    }

    for writer in writers {
        writer.join().expect("a writer thread panicked");
    }
    stop.store(true, Ordering::Relaxed);
    for reader in readers {
        reader.join().expect("a reader thread panicked");
    }

    // --- PROOF THE TREATMENT RAN. ---
    let total_writes = writes_ok.load(Ordering::Relaxed);
    assert_eq!(
        total_writes,
        WRITERS * per_writer * VERSIONS,
        "not every write was acknowledged, so a missing value below could be a write that never \
         happened rather than one that was lost"
    );
    assert!(reads_done.load(Ordering::Relaxed) > 0, "no concurrent read ran");

    // --- THE CHECK THAT CAN SEE A LOST WRITE: every key at its final version. ---
    let mut wrong = Vec::new();
    for w in 0..WRITERS {
        for index in (w * per_writer)..((w * per_writer) + per_writer) {
            let got = match runtime
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringGet { key: key_of(index) },
                })
                .response
            {
                CommandResponse::Bytes { value } => value,
                other => {
                    wrong.push(format!("{}: answered {other:?}", key_of(index)));
                    continue;
                }
            };
            if got.as_deref() != Some(versioned_value(index, VERSIONS).as_slice()) {
                if wrong.len() < 16 {
                    wrong.push(format!(
                        "{}: final read is {:?}, expected version {VERSIONS}",
                        key_of(index),
                        got.as_ref()
                            .map(|v| String::from_utf8_lossy(&v[..v.len().min(12)]).to_string())
                    ));
                }
            }
        }
    }

    let problems = problems.lock().expect("problem lock");
    eprintln!(
        "  [evict-write-race] {total_writes} writes, {} reads, {ROUNDS} rounds: \
{released} buckets released, {refused} refused, {} lost-or-stale finals, {} in-flight problems",
        reads_done.load(Ordering::Relaxed),
        wrong.len(),
        problems.len(),
    );

    assert!(
        problems.is_empty(),
        "{} problems while writing and reading through an eviction cycle: first {:?}",
        problems.len(),
        &problems[..problems.len().min(5)]
    );
    assert!(
        wrong.is_empty(),
        "{} keys did not hold their final acknowledged write after an eviction cycle -- a write \
         was LOST or a stale version was served: first {:?}",
        wrong.len(),
        &wrong[..wrong.len().min(5)]
    );
}

/// The in-memory record-cache budget the shipped `config/temporalstore.toml` sets
/// (`cache_memory_bytes = 16777216`) and the one `seeded_runtime` above passes to
/// `with_local_dirs`. Named because the threshold decision is a comparison against it.
const SHIPPED_CACHE_MEMORY_BUDGET: u64 = 16 * 1024 * 1024;

/// WHOSE CACHE THE EVICT GATE IS READING. The decisive control for the threshold decision.
///
/// `apply_storage_eviction` builds its gate input from `storage_cache_inspection_report(shard_id)`
/// `.stats`, and that field is `MultiLayerCache::stats()` -- which folds every CACHE shard and is
/// NOT filtered by the TemporalStore `shard_id` it was asked about. Reading the code says the cache
/// terms are process-wide; this runs it.
///
/// TWO SHARDS, DELIBERATELY LOPSIDED: shard 1 holds a corpus, shard 2 holds ten keys. If the cache
/// terms were per-shard, shard 2's pressure would be its own ten keys' worth. If they are
/// process-wide, shard 2's pressure carries shard 1's cache.
///
/// WHY THIS IS STILL MEASURED NOW THAT THE GATE HAS BEEN NARROWED. The defect it found is fixed in
/// the gate, not in the cache: `MultiLayerCache::stats()` is still one node-wide number and still
/// the only aggregate the cache offers. This test is what keeps that fact measured, so that folding
/// any of it back into a per-shard decision is a change somebody has to argue against a figure.
/// `the_evict_gate_reads_only_this_shards_own_bytes` is the other half -- it asserts the gate's own
/// input carries none of this.
#[test]
fn the_evict_gates_cache_terms_are_process_wide_not_per_shard() {
    const BIG_KEYS: usize = 8_000;
    const SMALL_KEYS: usize = 10;

    let dir = tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        SHIPPED_CACHE_MEMORY_BUDGET as usize,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    for shard_id in [1u32, 2u32] {
        let response = engine.load_shard_with(crate::control::LoadShardRequest {
            shard_id: shard_id as ShardId,
            table_name: format!("evict-live-{shard_id}"),
            shard_uri: format!("local://evict-live/{shard_id}"),
            start_routing_bucket: 0,
            end_routing_bucket: SHIPPED_END_BUCKET,
            readonly: false,
            load_version: 1,
            local_node_id: Some(1),
        });
        assert!(
            response.status.ok,
            "load shard {shard_id}: {:?}",
            response.status
        );
    }
    for (shard_id, keys) in [(1u32, BIG_KEYS), (2u32, SMALL_KEYS)] {
        for index in 0..keys {
            let response = engine.execute(ExecuteRequest {
                shard_id: shard_id as ShardId,
                command: Command::StringSet {
                    key: format!("s{shard_id}-{index:06}"),
                    value: keyed_value(index),
                },
            });
            assert!(
                response.status.ok,
                "write {shard_id}/{index}: {:?}",
                response.status
            );
        }
    }
    let runtime = DataNodeRuntime::new_without_workers_with_options(
        engine,
        DataNodeRuntimeOptions {
            worker_threads: 0,
            max_queue_depth: 64,
            max_background_queue_depth: 8,
        },
    );
    let engine = runtime.engine();
    let options = proposed_options();

    // Warm the cache from SHARD 1 ONLY. Writes do not fill the record cache -- reads do -- so this
    // is what puts bytes in the cache terms at all, and doing it on one shard is what makes the
    // comparison below able to say whose bytes they are.
    warm_cache_by_reading(&engine, 1, "s1-", BIG_KEYS);

    let (big, _) = runtime.storage_manager_pressure_snapshot(1, &options);
    let (small, _) = runtime.storage_manager_pressure_snapshot(2, &options);
    let big_index = engine.bucket_index_resident_bytes(1);
    let small_index = engine.bucket_index_resident_bytes(2);

    eprintln!(
        "  [evict-gate-scope] shard 1 ({BIG_KEYS} keys): pressure={} cache_mem={} cache_disk={} index={}",
        big.eviction_memory_pressure_bytes, big.cache_memory_bytes, big.cache_disk_bytes, big_index,
    );
    eprintln!(
        "  [evict-gate-scope] shard 2 ({SMALL_KEYS} keys): pressure={} cache_mem={} cache_disk={} index={}",
        small.eviction_memory_pressure_bytes,
        small.cache_memory_bytes,
        small.cache_disk_bytes,
        small_index,
    );

    // PROOF THE FIXTURE IS LOPSIDED, before reading anything out of the comparison. Without this a
    // run where both shards ended up the same size would "confirm" process-wide terms vacuously.
    assert!(
        big_index > small_index.saturating_mul(4),
        "the two shards' indexes are not lopsided ({big_index} vs {small_index}), so this fixture \
         cannot tell a process-wide cache term from a per-shard one"
    );
    // PROOF THE CACHE IS WARM. Without this the equality below holds trivially at zero, which is
    // exactly what the first version of this test measured and nearly published.
    assert!(
        big.cache_memory_bytes > 0,
        "the cache memory term is zero after warming {BIG_KEYS} reads on shard 1, so this test \
         compares two zeroes and proves nothing about whose cache the gate reads"
    );

    // THE FINDING. Both shards report the SAME cache terms, which are therefore not theirs.
    assert_eq!(
        small.cache_memory_bytes, big.cache_memory_bytes,
        "the cache memory term differs between shards, so it IS per-shard and the process-wide \
         reading of `MultiLayerCache::stats()` is wrong -- re-argue the threshold from that"
    );
    assert_eq!(
        small.cache_disk_bytes, big.cache_disk_bytes,
        "the cache disk term differs between shards, so it IS per-shard"
    );
    // And the consequence, stated as arithmetic: the ten-key shard's pressure is mostly the OTHER
    // shard's cache.
    assert!(
        small.cache_memory_bytes.saturating_add(small.cache_disk_bytes) > small_index,
        "the ten-key shard's pressure is dominated by its own index after all ({} cache vs {} \
         index), which would make a constant threshold safer than this test claims",
        small.cache_memory_bytes.saturating_add(small.cache_disk_bytes),
        small_index,
    );
    eprintln!(
        "    => the ten-key shard sits beside {} B of cache that is not its own, against {} B of \
its own resident index. A gate that read the node-wide cache would therefore open on a shard \
holding ten records; the gate reads {} B, which is this shard's index alone.",
        small.cache_memory_bytes.saturating_add(small.cache_disk_bytes),
        small_index,
        small.eviction_memory_pressure_bytes,
    );
    // AND THAT LAST NUMBER IS THE SHARD'S OWN, asserted here so this test fails if the node-wide
    // terms are folded back into the published signal. The gate's own input is asserted in
    // `the_evict_gate_reads_only_this_shards_own_bytes`; this is the published mirror of it.
    assert_eq!(
        small.eviction_memory_pressure_bytes, small_index,
        "the published eviction signal for a ten-key shard is {} B while its own resident index is \
         {} B, so the signal has picked the node-wide cache terms back up",
        small.eviction_memory_pressure_bytes, small_index,
    );
}

/// WHAT A DEFAULT THRESHOLD HAS TO CLEAR, measured warm AND cold at four corpus sizes.
///
/// READ THE `cold_index` COLUMN; IT IS THE ONLY ONE THE GATE SEES. The gate's input is
/// `bucket_index_resident_bytes` alone, so the cache columns here are no longer terms of the number
/// a threshold is compared against -- they are kept because the asymmetry they show is why the
/// narrowing was needed: the cache is capped by its own budget and evicts to stay there, the index
/// is capped by nothing. A threshold set against a warm cache would have been set against a term
/// that cannot grow; set against the index it is set against the term that can.
///
/// `what_pressure_a_shard_carries_at_three_corpus_sizes` above drops the cache first, so every byte
/// it reports is index. That is the right way to see the index slope and the wrong way to pick a
/// threshold, because the state a threshold has to NOT fire in is the state the writes left: cache
/// warm, nobody asking for anything to be relieved.
///
/// So each size is read twice from the same store -- WARM exactly as seeded, then COLD after
/// `invalidate_shard` -- and the budget the cache is allowed to fill is printed beside both.
#[test]
fn what_a_default_eviction_threshold_has_to_clear() {
    // THREE sizes, a decade apart at the ends. Not four: every size is seeded AND read back, and
    // the point of the table is the slope plus the comparison against the cache budget, neither of
    // which a fourth point settles. The extrapolation to a corpus that would reach a candidate
    // threshold is stated from this slope rather than measured, and says so.
    const SIZES: [usize; 3] = [2_000, 8_000, 20_000];

    struct Row {
        keys: usize,
        warm_total: u64,
        warm_cache: u64,
        warm_index: u64,
        cold_total: u64,
        cold_index: u64,
    }

    let mut rows: Vec<Row> = Vec::new();
    for keys in SIZES {
        let dir = tempdir().unwrap();
        let runtime = seeded_runtime(dir.path(), keys);
        let engine = runtime.engine();
        let options = proposed_options();

        // WARM MEANS READ-WARM. Seeding alone leaves both cache terms at zero.
        warm_cache_by_reading(&engine, 1, "evictlive-", keys);
        let (warm, _) = runtime.storage_manager_pressure_snapshot(1, &options);
        let warm_index = engine.bucket_index_resident_bytes(1);
        assert!(
            warm.cache_memory_bytes > 0,
            "{keys} keys: the cache term is zero after reading every key back, so the warm arm is \
             not warm and its comparison against the budget means nothing"
        );
        let _ = engine.cache().invalidate_shard(1);
        let (cold, _) = runtime.storage_manager_pressure_snapshot(1, &options);
        let cold_index = engine.bucket_index_resident_bytes(1);

        assert!(
            warm.eviction_memory_pressure_bytes > 0,
            "{keys} keys produced no warm pressure"
        );
        assert!(cold_index > 0, "{keys} keys produced no resident index");
        rows.push(Row {
            keys,
            warm_total: warm.eviction_memory_pressure_bytes,
            warm_cache: warm.cache_memory_bytes.saturating_add(warm.cache_disk_bytes),
            warm_index,
            cold_total: cold.eviction_memory_pressure_bytes,
            cold_index,
        });
    }

    eprintln!(
        "  [evict-threshold-bar] shipped cache_memory_bytes budget = {SHIPPED_CACHE_MEMORY_BUDGET}"
    );
    eprintln!(
        "    {:>6} {:>12} {:>12} {:>12} {:>9} {:>12} {:>12} {:>9}",
        "keys",
        "warm_total",
        "warm_cache",
        "warm_index",
        "warm_B/k",
        "cold_total",
        "cold_index",
        "cold_B/k"
    );
    for row in &rows {
        eprintln!(
            "    {:>6} {:>12} {:>12} {:>12} {:>9.1} {:>12} {:>12} {:>9.1}",
            row.keys,
            row.warm_total,
            row.warm_cache,
            row.warm_index,
            row.warm_total as f64 / row.keys as f64,
            row.cold_total,
            row.cold_index,
            row.cold_total as f64 / row.keys as f64,
        );
    }
    let biggest = rows.last().expect("three sizes");
    eprintln!(
        "    => at {} keys THIS SHARD's gate input is {} B of resident index, which is {:.4}x the \
node's whole cache budget. The gate no longer reads that budget, so the comparison is published \
for scale rather than as the threshold argument: the index is the term that grows with the shard.",
        biggest.keys,
        biggest.cold_index,
        biggest.cold_index as f64 / SHIPPED_CACHE_MEMORY_BUDGET as f64,
    );

    // THE SHAPE, asserted on the index term, which is the only one eviction can reduce.
    assert!(
        rows.windows(2).all(|pair| pair[0].cold_index < pair[1].cold_index),
        "the resident index did not grow with the corpus: {:?}",
        rows.iter()
            .map(|row| (row.keys, row.cold_index))
            .collect::<Vec<_>>()
    );
    // AND THE TERM THAT DOES NOT GROW WITH THE SHARD. The cache is capped by its own budget and
    // evicts to stay there; the index is not capped by anything. That asymmetry is the whole reason
    // a default threshold is hard, so it is asserted rather than left to the table to imply.
    assert!(
        rows.iter()
            .all(|row| row.warm_cache <= SHIPPED_CACHE_MEMORY_BUDGET),
        "a cache term exceeded the budget it was given, so the cache is not self-capping and the \
         threshold argument below has to be redone: {:?}",
        rows.iter()
            .map(|row| (row.keys, row.warm_cache))
            .collect::<Vec<_>>()
    );
}

/// WHAT THE GATE COSTS WHEN IT DECLINES -- the per-round price of switching `enable_evict` on.
///
/// With the stage enabled, every maintenance round on every loaded shard computes the gate's input
/// before it can decline. That input is built from `storage_cache_inspection_report`, whose listing
/// is `MultiLayerCache::entries_for_shard`: it materialises and SORTS one entry per cached record
/// and falls through to a filesystem `metadata()` for every entry the disk index does not hold.
/// None of that is gate input -- the gate reads `.stats` (aggregate counters) and the bucket-index
/// walk, and the per-entry `entries`/`bucket_summaries` are used only AFTER the gate opens.
///
/// COUNTED, NOT TIMED, on purpose: this box is shared and a wall-clock figure moves with whatever
/// else is compiling, while a listing count does not. See the counter's own doc.
///
/// WHAT MAKES THIS FAIL RATHER THAN BE VACUOUS:
///   * the control arm asserts the round DID run the stage and the gate DID decline, so a pass
///     cannot come from the stage being skipped for some other reason;
///   * the cache is warm and non-empty, asserted, so a zero listing count cannot come from there
///     being nothing to list;
///   * both a must-find and a must-not-find number are checked -- the index walk must still happen
///     (the gate needs it) and the cache listing must not.
#[test]
fn the_declining_gate_pays_no_cache_listing() {
    const KEYS: usize = 4_000;

    let dir = tempdir().unwrap();
    let runtime = seeded_runtime(dir.path(), KEYS);
    let engine = runtime.engine();

    // Warm and non-empty: the listing this test says is not paid has something to return. Reads,
    // not the writes above -- the write path leaves the record cache empty.
    warm_cache_by_reading(&engine, 1, "evictlive-", KEYS);
    let cache_entries = engine.cache().entries_for_shard(1).len();
    assert!(
        cache_entries > 0,
        "the shard's cache is empty, so a zero listing count below would prove nothing"
    );

    // A threshold ABOVE the shard's own pressure, so the gate declines. Read from the shard rather
    // than written as a literal: a literal would stop being above the pressure the moment the
    // per-key cost moved, and the test would quietly start measuring the open-gate path instead.
    let probe_options = proposed_options();
    let (pressure, _) = runtime.storage_manager_pressure_snapshot(1, &probe_options);
    let closing_threshold = pressure.eviction_memory_pressure_bytes.saturating_mul(4) + 1;
    let options = StorageManagerOptions {
        enable_evict: true,
        eviction_dump_before_evict: true,
        eviction_memory_pressure_threshold: closing_threshold,
        // PINNED to 0; see `proposed_options`. This arm is about the WIDE gate declining, and at
        // the shipped bound it would decline as the bound instead, which pays a different price.
        index_resident_limit_bytes: 0,
        ..StorageManagerOptions::default()
    };

    crate::engine::reset_storage_cache_inspection_listings();
    crate::engine::reset_bucket_index_resident_bytes_visits();
    let report = runtime.run_storage_manager_once(1, options);
    let listings = crate::engine::storage_cache_inspection_listings();
    let listed_entries = crate::engine::storage_cache_inspection_listing_entries();
    let index_visits = crate::engine::bucket_index_resident_bytes_visits();

    // THE POSITIVE ARM, and the reason the zero below is not vacuous. The counter is thread-local,
    // so a listing made on another thread would be invisible and an un-instrumented round would
    // read zero exactly like an instrumented one that did not list. This drives the OPEN gate --
    // threshold 0, the same round, the same thread, the same engine -- and asserts the counter sees
    // its listing. Run AFTER the declining arm so it cannot perturb it.
    crate::engine::reset_storage_cache_inspection_listings();
    let open_report = runtime.run_storage_manager_once(
        1,
        StorageManagerOptions {
            enable_evict: true,
            eviction_dump_before_evict: true,
            eviction_memory_pressure_threshold: 0,
            // PINNED to 0; see `proposed_options`. This arm's whole job is to OPEN the wide gate,
            // which the shipped bound would close.
            index_resident_limit_bytes: 0,
            ..StorageManagerOptions::default()
        },
    );
    let open_listings = crate::engine::storage_cache_inspection_listings();
    let open_eviction = open_report
        .eviction
        .as_ref()
        .expect("the evict stage must report when it is enabled");
    assert_eq!(
        open_eviction.skipped_reason, "",
        "the positive arm's gate did not open at threshold 0, so it cannot show the counter seeing \
         a listing: reason={:?}",
        open_eviction.skipped_reason
    );
    assert!(
        open_listings > 0,
        "the OPEN-gate round made no cache listing this thread could see, so the counter is not \
         wired to the round at all and the zero asserted below proves nothing"
    );

    let eviction = report
        .eviction
        .as_ref()
        .expect("the evict stage must report when it is enabled");

    eprintln!(
        "  [evict-gate-cost] {KEYS} keys, {cache_entries} cache entries, threshold={} \
pressure_before={} -- DECLINING round: cache listings={} listed_entries={} \
index_walk_visits={}; OPEN-gate control round: cache listings={}",
        eviction.memory_pressure_threshold,
        eviction.pressure_before,
        listings,
        listed_entries,
        index_visits,
        open_listings,
    );

    // PROOF THE ROUND IS THE ONE THIS TEST IS ABOUT, before reading the counters.
    assert!(
        report.executed_stages.iter().any(|stage| stage == "evict"),
        "the evict stage did not run: executed={:?} skipped={:?}",
        report.executed_stages,
        report.skipped_stages
    );
    assert_eq!(
        eviction.skipped_reason, "memory_pressure_below_threshold",
        "the gate did not decline, so this round is not the declining-gate case: reason={:?} \
pressure_before={} threshold={}",
        eviction.skipped_reason,
        eviction.pressure_before,
        eviction.memory_pressure_threshold,
    );
    assert!(
        eviction.selected_victims.is_empty(),
        "the declining gate still took {} victims",
        eviction.selected_victims.len()
    );

    // MUST BE FOUND: the gate's own input. The index walk is irreducible -- it IS the per-shard
    // term the gate exists to read -- so a zero here would mean the gate is not reading the index
    // at all and the whole signal has regressed.
    assert!(
        index_visits > 0,
        "the round made no bucket-index resident walk, so the gate did not read its index term"
    );

    // MUST NOT BE FOUND: the per-entry cache listing, which the gate does not need.
    assert_eq!(
        listings, 0,
        "a round whose evict gate DECLINED still listed the shard's cache {listings} time(s), \
         returning {listed_entries} entries. That listing is \
         `MultiLayerCache::entries_for_shard` -- a sort over one entry per cached record plus a \
         filesystem `metadata()` for every entry the disk index does not hold -- and the gate \
         reads none of it. Defaulting `enable_evict` on makes every loaded shard pay this on \
         every maintenance round to reach a decision that does not use it."
    );
}

/// THAT THE LOOP SETTLES: once eviction has fired, the quantity the gate reads falls BELOW the
/// threshold and the next round declines.
///
/// This is the property the signal was changed to have -- the gate reads
/// `bucket_index_resident_bytes` and not the node-only `_floor`, because the floor cannot fall when
/// a bucket is released and a gate on it would re-fire for ever. "Would converge" is an argument;
/// this runs rounds until the stage declines and prints the trajectory.
///
/// The threshold is set just BELOW the shard's measured pressure, so the first round's gate opens
/// and the only way the loop can stop is by the pressure actually falling. A threshold picked as a
/// literal could stop the loop by being unreachable rather than by convergence.
#[test]
fn the_eviction_loop_settles_below_its_threshold_instead_of_refiring() {
    const KEYS: usize = 4_000;
    const MAX_ROUNDS: usize = 40;

    let dir = tempdir().unwrap();
    let runtime = seeded_runtime(dir.path(), KEYS);
    let engine = runtime.engine();
    let _ = engine.cache().invalidate_shard(1);

    let probe = proposed_options();
    let (start, _) = runtime.storage_manager_pressure_snapshot(1, &probe);
    let start_pressure = start.eviction_memory_pressure_bytes;
    assert!(
        start_pressure > 2,
        "no pressure to converge from: {start_pressure}"
    );
    // Just under the shard's own pressure: the first gate must open.
    let threshold = start_pressure - 1;

    let options = StorageManagerOptions {
        enable_evict: true,
        eviction_dump_before_evict: true,
        eviction_memory_pressure_threshold: threshold,
        // PINNED to 0; see `proposed_options`. The convergence this measures is the WIDE gate's --
        // it stops on `memory_pressure_below_threshold`, a reason the bound's arm never reports.
        index_resident_limit_bytes: 0,
        ..StorageManagerOptions::default()
    };

    let mut trajectory: Vec<(usize, u64, usize, String)> = Vec::new();
    let mut settled_at: Option<usize> = None;
    let mut rounds_that_evicted = 0usize;
    for round in 1..=MAX_ROUNDS {
        // Cold between rounds, so the cache cannot be what makes the number fall: the only term
        // left moving is the resident index, which is the one eviction acts on.
        let _ = engine.cache().invalidate_shard(1);
        let report = runtime.run_storage_manager_once(1, options.clone());
        let eviction = report
            .eviction
            .as_ref()
            .expect("the evict stage must report when it is enabled");
        trajectory.push((
            round,
            eviction.pressure_before,
            eviction.selected_victims.len(),
            eviction.skipped_reason.clone(),
        ));
        if eviction.skipped_reason == "memory_pressure_below_threshold" {
            settled_at = Some(round);
            break;
        }
        rounds_that_evicted += 1;
    }

    eprintln!(
        "  [evict-convergence] {KEYS} keys, threshold={threshold} (start pressure {start_pressure})"
    );
    for (round, pressure, victims, reason) in &trajectory {
        eprintln!(
            "    round {round:>3}  pressure_before={pressure:>10}  victims={victims:>4}  \
reason={reason}"
        );
    }

    // PROOF THE LOOP ACTUALLY DID SOMETHING, so "settled" cannot mean "never started".
    assert!(
        rounds_that_evicted > 0,
        "no round ever evicted, so the gate was never open and nothing converged: {trajectory:?}"
    );
    let settled_at = settled_at.unwrap_or_else(|| {
        panic!(
            "the gate was still open after {MAX_ROUNDS} rounds, so the loop re-fires rather than \
             settling: {trajectory:?}"
        )
    });
    let final_pressure = trajectory
        .last()
        .map(|row| row.1)
        .expect("a trajectory with at least one round");
    assert!(
        final_pressure < threshold,
        "the loop stopped at pressure {final_pressure}, which is not below its threshold \
         {threshold} -- it stopped for some other reason than convergence"
    );
    eprintln!(
        "    => settled at round {settled_at}: pressure fell {start_pressure} -> {final_pressure}, \
below the threshold {threshold}"
    );
}

/// THE GATE'S OWN INPUT IS THIS SHARD'S BYTES, and the node-wide cache is not in it.
///
/// `the_evict_gates_cache_terms_are_process_wide_not_per_shard` above measures the contamination
/// that was: a ten-key shard beside an eight-thousand-key one sits next to 1,744,000 B of cache
/// that is not its own, against 1,200 B of its own resident index. While the gate summed
/// `MultiLayerCache::stats()` with the index term, that is what it decided on -- 99.93% of its
/// input belonged to another shard.
///
/// THIS READS THE NUMBER THE DECISION USES, not the published snapshot:
/// `StorageEvictionReport::pressure_before` straight off `apply_storage_eviction`. A snapshot that
/// had drifted from the gate could otherwise make this pass while the gate stayed wide. The
/// snapshot is checked too, at the end, against the gate's own figure.
///
/// THE FIXTURE HAS TO BE ABLE TO SEE THE DEFECT, and three clauses say so before anything is
/// concluded from it: the node's cache is warm, so the contaminating term is not zero; that cache
/// is several times the small shard's own index, so excluding it changes the answer; and the two
/// shards' indexes are lopsided, so a per-shard reading is distinguishable from a node-wide one
/// rather than equal to it by accident.
///
/// THE GATE IS READ ON ITS DECLINING PATH -- threshold `u64::MAX`, asserted to have declined -- so
/// the figure is a pure read and nothing is released while it is taken.
#[test]
fn the_evict_gate_reads_only_this_shards_own_bytes() {
    const BIG_KEYS: usize = 8_000;
    const SMALL_KEYS: usize = 10;

    let dir = tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        SHIPPED_CACHE_MEMORY_BUDGET as usize,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    for shard_id in [1u32, 2u32] {
        let response = engine.load_shard_with(crate::control::LoadShardRequest {
            shard_id: shard_id as ShardId,
            table_name: format!("evict-gate-{shard_id}"),
            shard_uri: format!("local://evict-gate/{shard_id}"),
            start_routing_bucket: 0,
            end_routing_bucket: SHIPPED_END_BUCKET,
            readonly: false,
            load_version: 1,
            local_node_id: Some(1),
        });
        assert!(
            response.status.ok,
            "load shard {shard_id}: {:?}",
            response.status
        );
    }
    for (shard_id, keys) in [(1u32, BIG_KEYS), (2u32, SMALL_KEYS)] {
        for index in 0..keys {
            let response = engine.execute(ExecuteRequest {
                shard_id: shard_id as ShardId,
                command: Command::StringSet {
                    key: format!("g{shard_id}-{index:06}"),
                    value: keyed_value(index),
                },
            });
            assert!(
                response.status.ok,
                "write {shard_id}/{index}: {:?}",
                response.status
            );
        }
    }
    // Warm from the BIG shard only, so the cache bytes have an owner and the comparison below can
    // say whose they are. Writes leave the record cache empty; reads are what fill it.
    warm_cache_by_reading(&engine, 1, "g1-", BIG_KEYS);

    let cache = engine.cache().stats();
    let node_cache_bytes = cache.memory_bytes.saturating_add(cache.disk_bytes);
    let big_index = engine.bucket_index_resident_bytes(1);
    let small_index = engine.bucket_index_resident_bytes(2);

    // REGIME 1: the contaminating term exists at all. Without this the exclusion below holds
    // trivially at zero.
    assert!(
        cache.memory_bytes > 0,
        "the node cache holds no memory bytes after warming {BIG_KEYS} reads on shard 1, so there \
         is nothing here for the gate to be shown excluding"
    );
    // REGIME 2: and it is large enough that excluding it changes the number.
    assert!(
        node_cache_bytes > small_index.saturating_mul(4),
        "the node cache ({node_cache_bytes} B) is not several times the ten-key shard's own index \
         ({small_index} B), so a gate reading both and a gate reading one report nearly the same \
         figure and this fixture cannot tell them apart"
    );
    // REGIME 3: the two shards are lopsided, so a per-shard reading is distinguishable.
    assert!(
        big_index > small_index.saturating_mul(4),
        "the two shards' indexes are not lopsided ({big_index} B vs {small_index} B), so a \
         per-shard gate input and a node-wide one could agree by accident"
    );

    let small_gate = engine.apply_storage_eviction(2, u64::MAX, 16, true, false);
    let big_gate = engine.apply_storage_eviction(1, u64::MAX, 16, true, false);
    // REGIME 4: both gates really declined, so `pressure_before` is a reading and not the state
    // left behind by a round that acted.
    assert_eq!(
        small_gate.skipped_reason, "memory_pressure_below_threshold",
        "the ten-key shard's gate did not decline at u64::MAX, so the figure below is not a pure \
         read: {small_gate:?}"
    );
    assert_eq!(
        big_gate.skipped_reason, "memory_pressure_below_threshold",
        "the eight-thousand-key shard's gate did not decline at u64::MAX: {big_gate:?}"
    );

    eprintln!(
        "  [evict-gate-narrow] node cache = {node_cache_bytes} B  |  shard 1 ({BIG_KEYS} keys): \
gate_input={} own_index={}  |  shard 2 ({SMALL_KEYS} keys): gate_input={} own_index={}",
        big_gate.pressure_before, big_index, small_gate.pressure_before, small_index,
    );

    // THE FINDING, on the shard the defect was measured on: its gate input is its own index, to the
    // byte, and the share attributable to anything else is zero.
    assert_eq!(
        small_gate.pressure_before, small_index,
        "the ten-key shard's gate reads {} B against {} B of its own resident index, so {} B of \
         its gate input belongs to something that is not this shard",
        small_gate.pressure_before,
        small_index,
        small_gate.pressure_before.saturating_sub(small_index),
    );
    // AND ON THE BIG SHARD, so the equality above is not an artefact of a shard small enough that
    // every number in sight rounds to the same place.
    assert_eq!(
        big_gate.pressure_before, big_index,
        "the eight-thousand-key shard's gate reads {} B against {} B of its own resident index",
        big_gate.pressure_before, big_index,
    );

    // AND THE PUBLISHED SIGNAL IS THE DECIDED ONE. These are built in different files -- the gate
    // in `storage_lifecycle_methods.rs`, the snapshot in `runtime_storage.rs` -- so nothing but
    // this makes them stay the same number.
    let runtime = DataNodeRuntime::new_without_workers_with_options(
        engine,
        DataNodeRuntimeOptions {
            worker_threads: 0,
            max_queue_depth: 64,
            max_background_queue_depth: 8,
        },
    );
    let options = proposed_options();
    let (small_snapshot, _) = runtime.storage_manager_pressure_snapshot(2, &options);
    assert_eq!(
        small_snapshot.eviction_memory_pressure_bytes, small_gate.pressure_before,
        "the published eviction signal ({} B) and the gate's own input ({} B) are different \
         numbers, so an operator reads one and the shard is decided on the other",
        small_snapshot.eviction_memory_pressure_bytes, small_gate.pressure_before,
    );
}

/// Read one seeded key back off an engine and say whether it is the value its key encodes.
///
/// `keyed_value` makes every value a function of its key, so a read resolved through the WRONG
/// page fails here instead of passing because the length matched.
fn reads_back_whole(engine: &TemporalEngine, index: usize) -> Result<(), String> {
    let key = key_of(index);
    let answer = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringGet { key: key.clone() },
    });
    if !answer.status.ok {
        return Err(format!("{key}: read failed {:?}", answer.status));
    }
    match answer.response {
        CommandResponse::Bytes { value: Some(value) } if value == keyed_value(index) => Ok(()),
        CommandResponse::Bytes { value: Some(value) } => Err(format!(
            "{key}: {} bytes that are not this key's value",
            value.len()
        )),
        CommandResponse::Bytes { value: None } => Err(format!("{key}: MISSING")),
        other => Err(format!("{key}: a string read answered {other:?}")),
    }
}

/// THE PERIODIC LOOP CAN NOW REACH `apply_index_resident_bound`, AND IT IS GATED ON ITSELF.
///
/// That call is the eviction gate whose signal is one per-shard quantity. Its only non-test caller
/// was inside `run_storage_manager_cycle`, which runs on an inbound `TaskRequest::StorageManager`;
/// the loop a server starts is `start_storage_manager_scheduler_for_all_shards`, and it had no
/// field to pass a bound in and no call to pass it to. So the narrow gate existed and nothing on
/// the shipped loop could open it.
///
/// THREE ARMS, BECAUSE "IT FIRED" IS SATISFIED BY A GATE THAT ALWAYS FIRES. `enable_evict` is OFF
/// in all of them -- pinned off, since it now ships on -- so nothing here can pass because the
/// wide stage ran, and the shipped bound is pinned off too so that arm A really has no bound:
///
///   A  no bound             -> no eviction report at all, and the round says `evict_disabled`
///   B  a bound ABOVE this shard's resident index -> refuses, releasing nothing
///   C  a bound BELOW it     -> acts, and the resident index falls
///
/// Arm B is the one that makes arm C mean something: without it, a gate wired to fire
/// unconditionally would pass arms A and C exactly as a correct one does.
#[test]
fn the_periodic_loop_reaches_the_index_resident_bound() {
    const KEYS: usize = 2_000;

    let dir = tempdir().unwrap();
    let runtime = seeded_runtime(dir.path(), KEYS);
    let engine = runtime.engine();
    // Cache out of the way, so every byte compared below is index.
    let _ = engine.cache().invalidate_shard(1);

    // REGIME 1: the stage flag is off AND the shipped bound is pinned away, so no arm can act
    // through the wide gate and no arm can act through a bound this test did not set. Both halves
    // are needed now that the flag and the bound both ship on: without the pin, arm A would get
    // the shipped 64 MiB bound and run the stage it is asserting does not run.
    let base = StorageManagerOptions {
        enable_evict: false,
        index_resident_limit_bytes: 0,
        ..StorageManagerOptions::default()
    };
    assert!(
        !base.enable_evict,
        "the arms below are supposed to run with the wide stage off"
    );
    assert_eq!(
        base.index_resident_limit_bytes, 0,
        "arm A asserts that no eviction report is produced at all, which needs BOTH gates shut"
    );
    // REGIME 2: RESTATED, as the assertion that stood here asked. It read
    // `StorageManagerOptions::default().index_resident_limit_bytes == 0` and said that a shipped
    // bound "is a behaviour change for every deployment and belongs in the same decision as the
    // eviction threshold default". It now ships, in that same decision, so the pin moves from "no
    // deployment gets a bound" to "the bound a deployment gets is the named constant, and it is
    // not the one this test's arms use".
    let shipped_bound = StorageManagerOptions::default().index_resident_limit_bytes;
    assert_eq!(
        shipped_bound,
        crate::engine::reports::DEFAULT_INDEX_RESIDENT_LIMIT_BYTES,
        "the periodic loop's shipped bound has drifted off the named constant"
    );
    assert_ne!(
        shipped_bound, base.index_resident_limit_bytes,
        "the pin above is no longer pinning anything away from the shipped default, so arms B and \
         C are no longer this test's doing"
    );
    // REGIME 3: there is a resident index for a bound to be about.
    let resident = engine.bucket_index_resident_bytes(1);
    assert!(
        resident > 0,
        "the fixture left no resident index, so no bound on it can do anything"
    );

    // --- ARM A: no bound, stage off. ---
    let off = runtime.run_storage_manager_once(1, base.clone());
    assert!(
        off.eviction.is_none(),
        "a round with no bound and the stage off still produced an eviction report: {:?}",
        off.eviction
    );
    assert!(
        off.skipped_stages.iter().any(|stage| stage == "evict_disabled"),
        "the round did not report the stage as skipped: skipped={:?} executed={:?}",
        off.skipped_stages,
        off.executed_stages
    );

    // --- ARM B: a bound ABOVE this shard's bytes must REFUSE. ---
    let resident_before_refusal = engine.bucket_index_resident_bytes(1);
    let above = StorageManagerOptions {
        index_resident_limit_bytes: resident_before_refusal.saturating_mul(4).saturating_add(1),
        ..base.clone()
    };
    let refused_round = runtime.run_storage_manager_once(1, above);
    let refused = refused_round
        .eviction
        .as_ref()
        .expect("a bound that is set must report, even when it refuses");
    assert_eq!(
        refused.mode, "index_resident_bound",
        "the round opened some other gate than the bound: mode={} reason={}",
        refused.mode, refused.skipped_reason
    );
    assert_eq!(
        refused.skipped_reason, "index_resident_below_bound",
        "a bound four times this shard's resident index did not refuse: reason={:?} \
         pressure_before={} threshold={}",
        refused.skipped_reason, refused.pressure_before, refused.memory_pressure_threshold
    );
    assert_eq!(
        refused.bucket_index_buckets_released, 0,
        "the refusing round released {} buckets",
        refused.bucket_index_buckets_released
    );

    // --- ARM C: a bound BELOW them must ACT. ---
    let resident_before = engine.bucket_index_resident_bytes(1);
    let limit = resident_before / 2;
    assert!(
        limit > 0 && limit < resident_before,
        "the fixture's resident index ({resident_before} B) is too small to put a bound under, so \
         arm C cannot be in the regime it is about"
    );
    let under = StorageManagerOptions {
        index_resident_limit_bytes: limit,
        ..base
    };
    let acted_round = runtime.run_storage_manager_once(1, under);
    let acted = acted_round
        .eviction
        .as_ref()
        .expect("the bound must report when it is set");
    assert_eq!(
        acted.mode, "index_resident_bound",
        "the acting round opened some other gate than the bound: mode={}",
        acted.mode
    );
    assert_eq!(
        acted.skipped_reason, "",
        "the bound refused a shard whose resident index ({resident_before} B) is over its limit \
         ({limit} B): reason={:?}",
        acted.skipped_reason
    );
    assert!(
        acted.bucket_index_buckets_released > 0,
        "the round opened the bound's gate and released nothing: victims={} refused={}",
        acted.selected_victims.len(),
        acted.bucket_index_release_refused
    );
    let resident_after = engine.bucket_index_resident_bytes(1);
    assert!(
        resident_after < resident_before,
        "the round released {} buckets and freed no index memory ({resident_before} -> \
         {resident_after})",
        acted.bucket_index_buckets_released
    );
    eprintln!(
        "  [index-bound-loop] {KEYS} keys, stage OFF throughout: no bound -> no stage; bound \
{} B -> refused, 0 released; bound {limit} B -> {} buckets released, resident index \
{resident_before} -> {resident_after}",
        refused.memory_pressure_threshold,
        acted.bucket_index_buckets_released,
    );
}

/// WHEN BOTH SETTINGS ARE SET, THE HIGHER BAR OPENS THE STAGE AND THE REPORT SAYS WHICH.
///
/// Since the evict signal was narrowed, `eviction_memory_pressure_threshold` and
/// `index_resident_limit_bytes` bound the same quantity: this shard's `bucket_index_resident_bytes`.
/// Two settings on one number cannot both gate it, so the stage takes the higher bar.
///
/// THE TWO ARMS ARE THE SAME SETTINGS EXCEPT FOR THE BOUND, each on its own freshly seeded store so
/// neither inherits the other's cleaning:
///
///   control    threshold 1, NO bound           -> the wide gate opens, because no shard holding a
///                                                 record is under 1
///   treatment  threshold 1, bound above the shard -> the BOUND is the higher bar and refuses
///
/// The control is what makes the treatment a finding rather than a tautology: it shows the round
/// WOULD have evicted at this threshold, so the refusal is the bound's doing.
#[test]
fn the_higher_bar_opens_the_stage_and_the_report_says_which() {
    const KEYS: usize = 1_000;

    // --- CONTROL: threshold 1, no bound. ---
    let control_dir = tempdir().unwrap();
    let control_runtime = seeded_runtime(control_dir.path(), KEYS);
    let control_engine = control_runtime.engine();
    let _ = control_engine.cache().invalidate_shard(1);
    let control_resident = control_engine.bucket_index_resident_bytes(1);
    // REGIME: a threshold of 1 is below this shard's own bytes, which is what makes it a bar no
    // shard holding anything can be under.
    assert!(
        control_resident > 1,
        "the control shard's resident index is {control_resident} B, so a threshold of 1 is not \
         below it and the control cannot show the gate opening"
    );
    let control_round = control_runtime.run_storage_manager_once(
        1,
        StorageManagerOptions {
            enable_evict: true,
            eviction_memory_pressure_threshold: 1,
            index_resident_limit_bytes: 0,
            ..StorageManagerOptions::default()
        },
    );
    let control = control_round
        .eviction
        .as_ref()
        .expect("the stage reports when it is enabled");
    assert_eq!(
        control.skipped_reason, "",
        "the control round did not evict at threshold 1, so it cannot show that the treatment's \
         refusal is the bound's doing: reason={:?} pressure_before={}",
        control.skipped_reason, control.pressure_before
    );
    assert_eq!(
        control.mode, "evict_cache",
        "the control round opened the bound's gate although no bound was set: mode={}",
        control.mode
    );

    // --- TREATMENT: the same threshold, plus a bound above the shard. ---
    let dir = tempdir().unwrap();
    let runtime = seeded_runtime(dir.path(), KEYS);
    let engine = runtime.engine();
    let _ = engine.cache().invalidate_shard(1);
    let resident = engine.bucket_index_resident_bytes(1);
    let bound = resident.saturating_mul(4).saturating_add(1);
    // REGIME: the bound really is the higher bar.
    assert!(bound > 1, "the bound must exceed the threshold it is tested against");
    let round = runtime.run_storage_manager_once(
        1,
        StorageManagerOptions {
            enable_evict: true,
            eviction_memory_pressure_threshold: 1,
            index_resident_limit_bytes: bound,
            ..StorageManagerOptions::default()
        },
    );
    let treatment = round
        .eviction
        .as_ref()
        .expect("the stage reports when it is enabled");

    // THE FINDING: the bound is the gate that ran.
    assert_eq!(
        treatment.mode, "index_resident_bound",
        "the stage took the threshold rather than the higher bound: mode={} threshold={}",
        treatment.mode, treatment.memory_pressure_threshold
    );
    // AND IT IS THE BAR THE REPORT PUBLISHES, so an operator is not shown the one that lost.
    assert_eq!(
        treatment.memory_pressure_threshold, bound,
        "the report publishes {} as the bar while the stage used {bound}",
        treatment.memory_pressure_threshold
    );
    // AND IT REFUSED, which is the behaviour difference the bound buys at this threshold.
    assert_eq!(
        treatment.skipped_reason, "index_resident_below_bound",
        "the stage acted although the shard is under its bound: reason={:?}",
        treatment.skipped_reason
    );
    assert_eq!(
        treatment.bucket_index_buckets_released, 0,
        "the refusing round released {} buckets",
        treatment.bucket_index_buckets_released
    );
    eprintln!(
        "  [which-bar] {KEYS} keys: threshold=1 alone -> gate OPEN (mode={}); threshold=1 with \
bound={bound} B over a {resident} B index -> mode={} reason={}, 0 released",
        control.mode, treatment.mode, treatment.skipped_reason,
    );
}

/// NOTHING ACKNOWLEDGED IS LOST WHEN THE BOUND RELEASES, INCLUDING ACROSS A RELOAD.
///
/// Eviction drops resident state, so the deliverable is not the byte count -- it is that a read
/// after the release still answers, and keeps answering after the shard comes back from disk.
///
/// FOUR THINGS ASSERTED, in this order, because the later ones mean nothing without the earlier:
///
///   1  THE ROUND ACTUALLY RELEASED. "Nothing was lost" is also what a round that did nothing
///      reports, and what a fixture whose gate never opened reports.
///   2  NOTHING WAS DROPPED. `dropped_object_count` counts records DELETED, and it is incremented
///      only on the `delete_drop` path -- which `apply_index_resident_bound` never takes. A
///      non-zero here would mean the round deleted records rather than releasing their pages, so
///      asserting it zero is the other half of the accounting: pages went, records stayed.
///   3  EVERY KEY READS BACK WHOLE, COLD. Both directions -- the mismatch list is empty AND the
///      verified count equals the corpus, so a loop that read nothing cannot pass.
///   4  AND AGAIN AFTER A RELOAD, on a fresh engine over the same directories, so the answer comes
///      from what is on disk rather than from anything the evicting process still held in memory.
#[test]
fn nothing_acknowledged_is_lost_when_the_bound_releases_and_the_shard_reloads() {
    const KEYS: usize = 1_200;
    const MAX_ROUNDS: usize = 8;

    let dir = tempdir().unwrap();
    let (released_total, dropped_total, resident_before, resident_after) = {
        // `seeded_runtime` asserts every write's status, so every key below is ACKNOWLEDGED.
        let runtime = seeded_runtime(dir.path(), KEYS);
        let engine = runtime.engine();
        let _ = engine.cache().invalidate_shard(1);
        let resident_before = engine.bucket_index_resident_bytes(1);
        assert!(
            resident_before > 0,
            "the fixture left no resident index to release"
        );
        let limit = resident_before / 2;
        assert!(
            limit > 0 && limit < resident_before,
            "the bound ({limit} B) is not under this shard's resident index ({resident_before} B), \
             so the round cannot be forced to act"
        );
        let options = StorageManagerOptions {
            enable_evict: false,
            index_resident_limit_bytes: limit,
            ..StorageManagerOptions::default()
        };

        let mut released_total = 0usize;
        let mut dropped_total = 0usize;
        for _ in 0..MAX_ROUNDS {
            let report = runtime.run_storage_manager_once(1, options.clone());
            if let Some(eviction) = report.eviction.as_ref() {
                released_total += eviction.bucket_index_buckets_released;
                dropped_total += eviction.dropped_object_count;
            }
            if released_total > 0 {
                break;
            }
        }
        let resident_after = engine.bucket_index_resident_bytes(1);

        // --- 1. THE ROUND ACTUALLY RELEASED. ---
        assert!(
            released_total > 0,
            "{MAX_ROUNDS} rounds at a bound of {limit} B over a {resident_before} B index released \
             no bucket, so nothing below is a statement about eviction"
        );
        assert!(
            resident_after < resident_before,
            "{released_total} buckets were released and the resident index did not fall \
             ({resident_before} -> {resident_after})"
        );

        // --- 2. AND NOTHING WAS DROPPED. ---
        assert_eq!(
            dropped_total, 0,
            "the bound's rounds deleted {dropped_total} records. This arm releases pages; only the \
             `delete_drop` path deletes, and `apply_index_resident_bound` does not take it"
        );

        // --- 3. EVERY KEY READS BACK WHOLE, COLD. ---
        let _ = engine.cache().invalidate_shard(1);
        let mut verified = 0usize;
        let mut wrong: Vec<String> = Vec::new();
        for index in 0..KEYS {
            match reads_back_whole(&engine, index) {
                Ok(()) => verified += 1,
                Err(problem) => {
                    if wrong.len() < 64 {
                        wrong.push(problem);
                    }
                }
            }
        }
        assert!(
            wrong.is_empty(),
            "{} of {KEYS} acknowledged keys did not read back after the bound released \
             {released_total} buckets: first few {:?}",
            wrong.len(),
            &wrong[..wrong.len().min(5)]
        );
        assert_eq!(
            verified, KEYS,
            "only {verified} of {KEYS} keys were verified, so the empty mismatch list above is a \
             loop that did not run rather than a store that answered"
        );

        // Write the index out while the buckets are released, so the reload below comes back from
        // the state eviction left rather than from one taken before it.
        engine.flush_shard_index(1);
        (released_total, dropped_total, resident_before, resident_after)
    };

    // --- 4. AND AGAIN AFTER A RELOAD. ---
    //
    // A fresh engine over the same directories, and ONLY after the first one is dropped: two
    // handles on one directory is its own failure mode and would make this prove something else.
    let reloaded = TemporalEngine::with_local_dirs(
        16 * 1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    let response = reloaded.load_shard_with(crate::control::LoadShardRequest {
        shard_id: 1,
        table_name: "evict-live".to_string(),
        shard_uri: "local://evict-live/1".to_string(),
        start_routing_bucket: 0,
        end_routing_bucket: SHIPPED_END_BUCKET,
        readonly: false,
        load_version: 2,
        local_node_id: Some(1),
    });
    assert!(
        response.status.ok,
        "the shard did not come back after the bound released {released_total} buckets: {:?}",
        response.status
    );
    // REGIME FOR THE RELOAD ARM: the shard rebuilt an index, so the reads below go through one.
    let reloaded_resident = reloaded.bucket_index_resident_bytes(1);
    assert!(
        reloaded_resident > 0,
        "the reloaded shard rebuilt no resident index, so its reads are not going through the \
         structure eviction released"
    );
    let mut verified = 0usize;
    let mut wrong: Vec<String> = Vec::new();
    for index in 0..KEYS {
        match reads_back_whole(&reloaded, index) {
            Ok(()) => verified += 1,
            Err(problem) => {
                if wrong.len() < 64 {
                    wrong.push(problem);
                }
            }
        }
    }
    assert!(
        wrong.is_empty(),
        "{} of {KEYS} acknowledged keys did not survive a reload taken after the bound released \
         {released_total} buckets: first few {:?}",
        wrong.len(),
        &wrong[..wrong.len().min(5)]
    );
    assert_eq!(
        verified, KEYS,
        "only {verified} of {KEYS} keys were verified after the reload, so the empty mismatch list \
         above is a loop that did not run"
    );
    eprintln!(
        "  [bound-correctness] {KEYS} acknowledged keys, {released_total} buckets released, \
{dropped_total} records dropped, resident index {resident_before} -> {resident_after}; all \
{KEYS} read back cold, and all {KEYS} again after a reload into a {reloaded_resident} B index"
    );
}

/// WHICH BUCKETS THE ROUND TAKES, on a fixture whose right answer is known AND whose wrong answer
/// is what a selector ignoring recency would give.
///
/// The shipped selector (`evict_sampled_lru`, on by default) scans the bucket index in ascending
/// routing-bucket order from a cursor, with a bounded budget, and ranks what it pooled by
/// `last_used_ms` with ties broken on bucket id. So the buckets it MEETS FIRST are the
/// lowest-numbered ones.
///
/// THE FIXTURE MAKES THE LOWEST-NUMBERED BUCKETS THE HOT ONES. Every key in them is read again
/// after a clock gap, so their recency is strictly later than every other bucket's. A selector that
/// ranked on scan order, or on bucket id, or on nothing at all, would take exactly those; a
/// selector that ranks on recency cannot take any of them while colder buckets are in its pool.
///
/// WHAT MAKES THIS FAIL RATHER THAN BE VACUOUS: the scan budget is asserted to reach past the hot
/// band, the gate is asserted to have opened, and a FULL batch is asserted to have been taken --
/// a round that took fewer victims than its batch limit did not have to choose between them.
#[test]
fn the_round_takes_the_coldest_buckets_it_scanned_not_the_first_ones() {
    const KEYS: usize = 4_000;
    const BATCH: usize = 16;
    /// The hot band. Wider than the sampler's minimum scan of `samples * batch_limit` = 80 would
    /// need to be "all hot", and narrower than it, so the pass meets hot AND cold buckets.
    const HOT_BUCKETS: usize = 48;

    let dir = tempdir().unwrap();
    let runtime = seeded_runtime(dir.path(), KEYS);
    let engine = runtime.engine();

    let mut occupied: Vec<u32> = (0..KEYS)
        .map(|index| engine.routing_bucket_for_key(1, &key_of(index)))
        .collect();
    occupied.sort_unstable();
    occupied.dedup();
    // REGIME 1: there are enough buckets for a hot band and a cold remainder the scan can reach.
    assert!(
        occupied.len() > HOT_BUCKETS * 2,
        "{KEYS} keys occupy only {} buckets, so a {HOT_BUCKETS}-bucket hot band leaves too little \
         cold for the selector to choose instead",
        occupied.len()
    );
    let hot: std::collections::BTreeSet<u32> = occupied[..HOT_BUCKETS].iter().copied().collect();

    // The clock is milliseconds, so a gap is needed or the re-reads could stamp the same value the
    // writes did and "hot" would not be hotter.
    std::thread::sleep(std::time::Duration::from_millis(5));
    let mut touched = 0usize;
    for index in 0..KEYS {
        let key = key_of(index);
        if hot.contains(&engine.routing_bucket_for_key(1, &key)) {
            let answer = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringGet { key: key.clone() },
            });
            assert!(answer.status.ok, "warming {key}: {:?}", answer.status);
            touched += 1;
        }
    }
    // REGIME 2: the hot band actually got touched.
    assert!(
        touched > 0,
        "no key landed in the {HOT_BUCKETS} lowest-numbered buckets, so nothing was made hot"
    );

    let report = engine.apply_storage_eviction(1, 0, BATCH, true, false);
    // REGIME 3: the gate opened, so there is a selection to read.
    assert_eq!(
        report.skipped_reason, "",
        "the gate declined at threshold 0, so no victims were selected: {report:?}"
    );
    // REGIME 4: and a FULL batch was taken, so the selector had to leave some buckets out.
    assert_eq!(
        report.selected_victims.len(),
        BATCH,
        "the round took {} victims against a batch limit of {BATCH}, so it did not have to choose \
         between the buckets it scanned and this fixture cannot tell a recency ranking from any \
         other",
        report.selected_victims.len()
    );

    // THE FINDING: not one victim is a bucket that was just read, although those are precisely the
    // buckets the scan meets first.
    let taken_hot: Vec<u32> = report
        .selected_victims
        .iter()
        .map(|victim| victim.routing_bucket)
        .filter(|bucket| hot.contains(bucket))
        .collect();
    assert!(
        taken_hot.is_empty(),
        "{} of {BATCH} victims are buckets read {touched} times just before the round: {:?}. The \
         selector is taking what it scanned first rather than what was used least recently",
        taken_hot.len(),
        taken_hot
    );
    let lowest_hot = occupied[0];
    eprintln!(
        "  [victim-choice] {KEYS} keys over {} buckets; the {HOT_BUCKETS} lowest-numbered \
(from {lowest_hot}) read {touched} times, then one round of {BATCH}: victims {:?} -- none of them \
hot, although the scan starts at bucket {lowest_hot}",
        occupied.len(),
        report
            .selected_victims
            .iter()
            .map(|victim| victim.routing_bucket)
            .collect::<Vec<_>>(),
    );
}

/// THE SHIPPED DEFAULT RUNS THE NINTH STAGE AND OPENS THE BOUND, NOT THE WIDE GATE.
///
/// This is the gate on the pairing. `enable_evict` ships true and
/// `eviction_memory_pressure_threshold` ships 0, and 0 is documented as "evicts whenever it runs":
/// the wide gate is `pressure_before < threshold`, which no shard holding a record satisfies. So
/// the flag on its own is an always-open release on every loaded shard, every round. What makes it
/// safe is that `index_resident_limit_bytes` ships at 64 MiB and the loop takes the HIGHER bar, so
/// the arm the shipped round actually takes is `apply_index_resident_bound`.
///
/// TWO ARMS ON TWO FRESHLY SEEDED STORES, because a refusal is also what a fixture that cannot
/// evict at all reports, and because an arm that has already been cleaned by the other arm's round
/// would refuse for the wrong reason:
///
///   treatment  the SHIPPED defaults, untouched -> the stage RUNS, mode is the bound, 0 released
///   control    the same with ONLY the bound zeroed -> the wide gate opens and victims are taken
///
/// The control is what makes the treatment a finding: it shows this shard WOULD have been evicted
/// at the shipped threshold, so the refusal is the bound's doing and not the fixture's.
#[test]
fn the_shipped_default_opens_the_bound_and_never_the_wide_gate() {
    const KEYS: usize = 1_000;

    let defaults = StorageManagerOptions::default();

    // --- REGIME, before anything is read out of a round. ---
    //
    // R1 the ninth stage is on.
    assert!(
        defaults.enable_evict,
        "enable_evict ships off, so this test is about a configuration nobody gets"
    );
    // R2 the bound is the named constant and is not disabled.
    assert_eq!(
        defaults.index_resident_limit_bytes,
        crate::engine::reports::DEFAULT_INDEX_RESIDENT_LIMIT_BYTES,
        "the shipped bound has drifted off the named constant"
    );
    assert!(
        defaults.index_resident_limit_bytes > 0,
        "the shipped bound is 0, which means DISABLED -- the wide gate at a threshold of 0 is then \
         the only bar, and it cannot close"
    );
    // R3 the bound is the HIGHER bar, which is the comparison `index_bound_is_the_gate` makes.
    assert!(
        defaults.index_resident_limit_bytes > defaults.eviction_memory_pressure_threshold,
        "the shipped bound ({}) is not above the shipped threshold ({}), so the round falls back \
         on the wide arm",
        defaults.index_resident_limit_bytes,
        defaults.eviction_memory_pressure_threshold
    );

    // --- TREATMENT: the shipped defaults, not one field changed. ---
    let dir = tempdir().unwrap();
    let runtime = seeded_runtime(dir.path(), KEYS);
    let engine = runtime.engine();
    // Cache out of the way, so the bytes compared are index bytes.
    let _ = engine.cache().invalidate_shard(1);

    // R4 there is a resident index, and it is UNDER the shipped bound. Asserted rather than
    // assumed: the conclusion below is "it declines because the shard is under the bound", and
    // that is a statement about this fixture as much as about the default.
    let resident_before = engine.bucket_index_resident_bytes(1);
    assert!(
        resident_before > 0,
        "the fixture left no resident index, so there is nothing for a bound to be about"
    );
    assert!(
        resident_before < defaults.index_resident_limit_bytes,
        "this {KEYS}-key fixture holds {resident_before} B of resident index, which is already at \
         or over the shipped bound of {} B -- so a refusal here would not be a refusal",
        defaults.index_resident_limit_bytes
    );

    let shipped_round = runtime.run_storage_manager_once(1, StorageManagerOptions::default());

    // CONCLUSION 1: the stage RAN. The enablement is the point of the change, and a bound that
    // works by the stage never running would be the old behaviour under a new name.
    assert!(
        shipped_round
            .executed_stages
            .iter()
            .any(|stage| stage == "evict"),
        "the ninth stage did not run under the shipped defaults: executed={:?} skipped={:?}",
        shipped_round.executed_stages,
        shipped_round.skipped_stages
    );
    let shipped = shipped_round
        .eviction
        .as_ref()
        .expect("a stage that ran must report");

    // CONCLUSION 2: and the gate it opened is the BOUND, never the wide one.
    assert_eq!(
        shipped.mode, "index_resident_bound",
        "the shipped round opened `{}` -- at the shipped threshold of 0 that gate cannot close, so \
         this is an unbounded release on every loaded shard",
        shipped.mode
    );
    assert_ne!(
        shipped.mode, "evict_cache",
        "the shipped round took the wide cache-eviction arm"
    );
    assert_ne!(
        shipped.mode, "delete_drop",
        "the shipped round took the arm that DELETES records to reclaim memory"
    );
    // AND THE BAR IT PUBLISHES IS THE ONE IT USED, so an operator is not shown the loser.
    assert_eq!(
        shipped.memory_pressure_threshold, defaults.index_resident_limit_bytes,
        "the round published {} as its bar while the bound is {}",
        shipped.memory_pressure_threshold, defaults.index_resident_limit_bytes
    );

    // CONCLUSION 3: and it RELEASED NOTHING and DROPPED NOTHING, which is what "bounded" means on
    // a shard under the bound.
    assert_eq!(
        shipped.skipped_reason, "index_resident_below_bound",
        "the shipped round acted on a {KEYS}-key shard: reason={:?} pressure_before={}",
        shipped.skipped_reason, shipped.pressure_before
    );
    assert_eq!(
        shipped.bucket_index_buckets_released, 0,
        "the shipped round released {} buckets from a {KEYS}-key shard",
        shipped.bucket_index_buckets_released
    );
    assert_eq!(
        shipped.dropped_object_count, 0,
        "the shipped round deleted {} records",
        shipped.dropped_object_count
    );
    assert!(
        shipped.selected_victims.is_empty(),
        "the shipped round chose {} victims",
        shipped.selected_victims.len()
    );
    assert_eq!(
        engine.bucket_index_resident_bytes(1),
        resident_before,
        "the resident index moved under a round that reported releasing nothing"
    );

    // --- CONTROL: the same defaults with ONLY the bound zeroed, on its OWN fresh store. ---
    //
    // This is the configuration a change that flipped the flag alone would have shipped.
    let control_dir = tempdir().unwrap();
    let control_runtime = seeded_runtime(control_dir.path(), KEYS);
    let control_engine = control_runtime.engine();
    let _ = control_engine.cache().invalidate_shard(1);
    let control_round = control_runtime.run_storage_manager_once(
        1,
        StorageManagerOptions {
            index_resident_limit_bytes: 0,
            ..StorageManagerOptions::default()
        },
    );
    let control = control_round
        .eviction
        .as_ref()
        .expect("the stage is enabled in the shipped defaults, so it reports here too");
    assert_eq!(
        control.mode, "evict_cache",
        "zeroing the bound did not fall back on the wide gate: mode={}",
        control.mode
    );
    assert_eq!(
        control.skipped_reason, "",
        "the wide gate DECLINED at the shipped threshold of 0, so the treatment's refusal cannot \
         be attributed to the bound: reason={:?} pressure_before={} threshold={}",
        control.skipped_reason, control.pressure_before, control.memory_pressure_threshold
    );
    assert!(
        !control.selected_victims.is_empty(),
        "the wide gate opened and took no victims, so this control cannot show what the bound \
         prevented"
    );

    eprintln!(
        "  [shipped-pair] {KEYS} keys, {resident_before} B resident index against a {} B bound: \
shipped defaults -> stage RAN, mode={} reason={}, {} released, {} dropped, {} victims. Same \
defaults with the bound zeroed -> mode={} reason=<open>, {} victims taken.",
        defaults.index_resident_limit_bytes,
        shipped.mode,
        shipped.skipped_reason,
        shipped.bucket_index_buckets_released,
        shipped.dropped_object_count,
        shipped.selected_victims.len(),
        control.mode,
        control.selected_victims.len(),
    );
}

/// A BOUNDED ROUND AT THE SHIPPED DEFAULTS RELEASES PAGES AND KEEPS EVERY RECORD.
///
/// `nothing_acknowledged_is_lost_when_the_bound_releases_and_the_shard_reloads` above proves this
/// for the bound reached with the stage flag OFF. The flag now ships ON, and that is a different
/// round: `eviction_dump_before_evict` and `eviction_batch_limit` are read, the stage is counted,
/// and `run_eviction_stage` loops on the bound rather than running once. So the property is
/// re-proved on the configuration that actually ships, with ONE field moved -- the bound lowered
/// under this shard's index, which is what a 1.7-million-key shard reaches on its own.
///
/// FOUR THINGS ASSERTED, IN THIS ORDER, because the later ones mean nothing without the earlier:
///
///   1  THE ROUND ACTUALLY RELEASED, and it was the BOUND's arm that did it. "Nothing was lost" is
///      also what a round that did nothing reports.
///   2  NOTHING WAS DROPPED. `dropped_object_count` counts records DELETED and is incremented only
///      inside the `delete_drop` branch, which `apply_index_resident_bound` never takes and which
///      ships off. Asserting it POSITIVE would force this fixture onto the arm that deletes
///      records, which is the opposite of what is being proved; asserting it ZERO is the other
///      half of the accounting -- pages went, records stayed.
///   3  EVERY KEY READS BACK WHOLE, COLD, in both directions: the mismatch list empty AND the
///      verified count equal to the corpus, so a loop that read nothing cannot pass.
///   4  AND AGAIN AFTER A RELOAD, on a fresh engine over the same directories and only after the
///      first is dropped, in both directions again.
#[test]
fn the_shipped_defaults_release_pages_and_keep_every_record_when_the_bound_is_crossed() {
    const KEYS: usize = 1_200;
    const MAX_ROUNDS: usize = 8;

    let dir = tempdir().unwrap();
    let (released_total, dropped_total, resident_before, resident_after, mode) = {
        // `seeded_runtime` asserts every write's status, so every key below is ACKNOWLEDGED.
        let runtime = seeded_runtime(dir.path(), KEYS);
        let engine = runtime.engine();
        let _ = engine.cache().invalidate_shard(1);

        // --- REGIME. ---
        let defaults = StorageManagerOptions::default();
        assert!(
            defaults.enable_evict,
            "the stage ships off, so this is not the shipped configuration"
        );
        let resident_before = engine.bucket_index_resident_bytes(1);
        assert!(
            resident_before > 0,
            "the fixture left no resident index to release"
        );
        let limit = resident_before / 2;
        assert!(
            limit > 0 && limit < resident_before,
            "the bound ({limit} B) is not under this shard's resident index ({resident_before} B), \
             so the round cannot be forced to act"
        );
        // AND THE BOUND IS STILL THE HIGHER BAR, so this round takes the bound's arm and not the
        // wide one. With the shipped threshold of 0 this holds for any non-zero limit; asserted
        // because the conclusion names the arm.
        assert!(
            limit > defaults.eviction_memory_pressure_threshold,
            "the lowered bound ({limit}) is not above the shipped threshold ({}), so this round \
             takes the wide arm and proves something else",
            defaults.eviction_memory_pressure_threshold
        );

        // THE SHIPPED DEFAULTS WITH EXACTLY ONE FIELD MOVED.
        let options = StorageManagerOptions {
            index_resident_limit_bytes: limit,
            ..StorageManagerOptions::default()
        };

        let mut released_total = 0usize;
        let mut dropped_total = 0usize;
        let mut mode = String::new();
        for _ in 0..MAX_ROUNDS {
            let report = runtime.run_storage_manager_once(1, options.clone());
            if let Some(eviction) = report.eviction.as_ref() {
                released_total += eviction.bucket_index_buckets_released;
                dropped_total += eviction.dropped_object_count;
                mode = eviction.mode.clone();
            }
            if released_total > 0 {
                break;
            }
        }
        let resident_after = engine.bucket_index_resident_bytes(1);

        // --- 1. THE ROUND ACTUALLY RELEASED, THROUGH THE BOUND'S ARM. ---
        assert_eq!(
            mode, "index_resident_bound",
            "the rounds opened `{mode}` rather than the bound, so what follows is not a statement \
             about the bound"
        );
        assert!(
            released_total > 0,
            "{MAX_ROUNDS} rounds at a bound of {limit} B over a {resident_before} B index released \
             no bucket, so nothing below is a statement about eviction"
        );
        assert!(
            resident_after < resident_before,
            "{released_total} buckets were released and the resident index did not fall \
             ({resident_before} -> {resident_after})"
        );

        // --- 2. AND NOTHING WAS DROPPED. ---
        assert_eq!(
            dropped_total, 0,
            "the bound's rounds deleted {dropped_total} records. This arm releases pages; only the \
             `delete_drop` branch deletes, it ships off, and `apply_index_resident_bound` does not \
             take it"
        );

        // --- 3. EVERY KEY READS BACK WHOLE, COLD, BOTH DIRECTIONS. ---
        let _ = engine.cache().invalidate_shard(1);
        let mut verified = 0usize;
        let mut wrong: Vec<String> = Vec::new();
        for index in 0..KEYS {
            match reads_back_whole(&engine, index) {
                Ok(()) => verified += 1,
                Err(problem) => {
                    if wrong.len() < 64 {
                        wrong.push(problem);
                    }
                }
            }
        }
        assert!(
            wrong.is_empty(),
            "{} of {KEYS} acknowledged keys did not read back after the shipped round released \
             {released_total} buckets: first few {:?}",
            wrong.len(),
            &wrong[..wrong.len().min(5)]
        );
        assert_eq!(
            verified, KEYS,
            "only {verified} of {KEYS} keys were verified, so the empty mismatch list above is a \
             loop that did not run rather than a store that answered"
        );

        // Write the index out while the buckets are released, so the reload comes back from the
        // state eviction left rather than from one taken before it.
        engine.flush_shard_index(1);
        (
            released_total,
            dropped_total,
            resident_before,
            resident_after,
            mode,
        )
    };

    // --- 4. AND AGAIN AFTER A RELOAD, BOTH DIRECTIONS. ---
    //
    // A fresh engine over the same directories, and ONLY after the first one is dropped: two
    // handles on one directory is its own failure mode and would make this prove something else.
    let reloaded = TemporalEngine::with_local_dirs(
        16 * 1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    let response = reloaded.load_shard_with(crate::control::LoadShardRequest {
        shard_id: 1,
        table_name: "evict-live".to_string(),
        shard_uri: "local://evict-live/1".to_string(),
        start_routing_bucket: 0,
        end_routing_bucket: SHIPPED_END_BUCKET,
        readonly: false,
        load_version: 2,
        local_node_id: Some(1),
    });
    assert!(
        response.status.ok,
        "the shard did not come back after the shipped round released {released_total} buckets: \
         {:?}",
        response.status
    );
    let reloaded_resident = reloaded.bucket_index_resident_bytes(1);
    assert!(
        reloaded_resident > 0,
        "the reloaded shard rebuilt no resident index, so its reads are not going through the \
         structure eviction released"
    );
    let mut verified = 0usize;
    let mut wrong: Vec<String> = Vec::new();
    for index in 0..KEYS {
        match reads_back_whole(&reloaded, index) {
            Ok(()) => verified += 1,
            Err(problem) => {
                if wrong.len() < 64 {
                    wrong.push(problem);
                }
            }
        }
    }
    assert!(
        wrong.is_empty(),
        "{} of {KEYS} acknowledged keys did not survive a reload taken after the shipped round \
         released {released_total} buckets: first few {:?}",
        wrong.len(),
        &wrong[..wrong.len().min(5)]
    );
    assert_eq!(
        verified, KEYS,
        "only {verified} of {KEYS} keys were verified after the reload, so the empty mismatch list \
         above is a loop that did not run"
    );
    eprintln!(
        "  [shipped-bound-correctness] {KEYS} acknowledged keys, mode={mode}, {released_total} \
buckets released, {dropped_total} records dropped, resident index {resident_before} -> \
{resident_after}; all {KEYS} read back cold, and all {KEYS} again after a reload into a \
{reloaded_resident} B index"
    );
}

/// WHAT SCALE THE SHIPPED BOUND IS A BOUND AT, derived from a two-point slope rather than quoted.
///
/// A byte figure on its own says nothing about when a bound fires, and the obvious way to get a
/// per-key figure -- divide one fixture's resident index by its key count -- is the wrong one here.
/// On the shipped `0..1023` routing range a small shard's resident index is dominated by the FIXED
/// cost of having bucket nodes at all, so that ratio reads several times the marginal cost and
/// would put the bound at a key count several times too low. Two sizes and a slope separates the
/// two terms.
///
/// TWO CONCLUSIONS, each from the same pair of measurements and each in its own block:
///
///   1  THE SCALE IT FIRES AT. The marginal cost times a key count equals 64 MiB somewhere around
///      a million and a half keys in ONE shard, which is the claim the field doc makes.
///   2  AND THAT NO FIXTURE REACHES IT. Projected from the same slope, a shard of 100,000 keys --
///      five times the largest corpus any fixture in this file seeds -- is still under the bound.
///      That is the property that lets the bound ship as a default without re-goldening the
///      suite's eviction measurements, so it is asserted rather than assumed.
#[test]
fn the_default_bound_is_far_above_any_fixture_and_reached_at_a_known_scale() {
    const SMALL: usize = 2_000;
    const LARGE: usize = 6_000;
    /// Five times the largest corpus any fixture in this file seeds.
    const FIXTURE_CEILING: usize = 100_000;

    let limit = crate::engine::reports::DEFAULT_INDEX_RESIDENT_LIMIT_BYTES;
    assert_eq!(
        StorageManagerOptions::default().index_resident_limit_bytes,
        limit,
        "the periodic loop's bound is not the constant this test derives a scale for"
    );

    let measure = |keys: usize| -> u64 {
        let dir = tempdir().unwrap();
        let runtime = seeded_runtime(dir.path(), keys);
        let engine = runtime.engine();
        // Cold, so the number is index and not cache wearing a new label.
        let _ = engine.cache().invalidate_shard(1);
        let resident = engine.bucket_index_resident_bytes(1);
        assert!(resident > 0, "{keys} keys produced no resident index");
        resident
    };

    let small = measure(SMALL);
    let large = measure(LARGE);

    // --- REGIME: the index grows with the corpus, or there is no slope to take. ---
    assert!(
        large > small,
        "the resident index did not grow between {SMALL} keys ({small} B) and {LARGE} keys \
         ({large} B), so no marginal cost can be derived"
    );
    let marginal = (large - small) as f64 / (LARGE - SMALL) as f64;
    assert!(
        marginal > 0.0,
        "the marginal cost came out at {marginal} B/key"
    );
    // AND THE RATIO REALLY IS THE MISLEADING ONE, which is why the slope is taken. Asserted so the
    // prose above is a measured claim rather than an explanation of a choice.
    let ratio_at_small = small as f64 / SMALL as f64;
    assert!(
        ratio_at_small > marginal,
        "the per-key ratio at {SMALL} keys ({ratio_at_small:.1} B/key) is not above the marginal \
         cost ({marginal:.1} B/key), so the fixed node term this test is separating out is not \
         there and the slope buys nothing"
    );

    // --- CONCLUSION 1: the scale the bound fires at. ---
    //
    // Solved from the measured line rather than from the slope alone, so the fixed term is carried
    // rather than dropped.
    let keys_at_bound = SMALL as f64 + (limit.saturating_sub(small)) as f64 / marginal;
    assert!(
        keys_at_bound > 500_000.0 && keys_at_bound < 10_000_000.0,
        "the shipped bound of {limit} B is reached at {keys_at_bound:.0} keys in one shard, which \
         is outside the band this default was chosen in. At the low end it would fire on shards a \
         deployment really runs; at the high end it is not a bound at all. Re-derive the default."
    );

    // --- CONCLUSION 2: and no fixture-scale shard reaches it. ---
    let projected_at_ceiling =
        small as f64 + marginal * (FIXTURE_CEILING - SMALL) as f64;
    assert!(
        projected_at_ceiling < limit as f64,
        "a {FIXTURE_CEILING}-key shard projects to {projected_at_ceiling:.0} B of resident index, \
         at or over the shipped bound of {limit} B. Every fixture that forces the evict gate open \
         would then be refused by the bound instead, and the suite's eviction measurements would \
         have to be re-derived rather than pinned."
    );

    eprintln!(
        "  [bound-scale] resident index {small} B at {SMALL} keys, {large} B at {LARGE} keys \
=> marginal {marginal:.1} B/key (ratio at {SMALL} keys reads {ratio_at_small:.1} B/key, \
{:.1}x the marginal, which is the fixed node term). The {limit} B bound is reached at \
{keys_at_bound:.0} keys in one shard; a {FIXTURE_CEILING}-key shard projects to \
{projected_at_ceiling:.0} B, {:.1}% of the bound.",
        ratio_at_small / marginal,
        projected_at_ceiling / limit as f64 * 100.0,
    );
}
