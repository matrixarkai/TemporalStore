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

/// The configuration a default-on change would ship, so the gates below run that rather than a
/// hand-tuned neighbour of it.
///
/// NOT the shipped default: this file lands before any default moves, and the threshold here is 1 so
/// the fixtures' own pressure clears it. A real default needs a threshold the cache alone cannot
/// clear, which is a separate decision with its own evidence.
fn proposed_options() -> StorageManagerOptions {
    StorageManagerOptions {
        enable_evict: true,
        eviction_dump_before_evict: true,
        eviction_memory_pressure_threshold: 1,
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
