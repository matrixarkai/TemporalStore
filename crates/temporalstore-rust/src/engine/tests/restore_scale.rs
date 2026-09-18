// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHAT BRINGING A SHARD BACK COSTS, at two corpus sizes, phase by phase.
//!
//! The restore path has the best correctness coverage in this engine and nobody had counted what
//! it costs. It is also the path an incident actually runs: a shard that took writes, dumped a
//! checkpoint, took more writes and then went away has to be rebuilt from the checkpoint plus the
//! log tail, and the only thing anyone could say about that was how long it took on the box it
//! happened to run on.
//!
//! `load_shard_with` is four jobs in a row. `restore_phase_probe` cuts it at the boundaries
//! between them and emits a marker at each, so an `strace` can be SPLIT at the same places the
//! allocation counts are: the rows below sum to the span total by construction, residual 0 at
//! both sizes.
//!
//! THE BILL. 2,000 and 20,000 records of 128 incompressible bytes, half of each corpus covered by
//! a durable dump manifest and half left as a WAL tail to replay. One process per size:
//!
//! ```text
//!   phase                       allocations             file+desc syscalls          bytes read
//!                            2,000    20,000  ratio    2,000  20,000  ratio    2,000     20,000
//!   manifest_and_base_index 17,591   174,656   9.93x      12      12   1.00x  164,318  1,875,686
//!   publish_and_seed        10,256   102,146   9.96x      28     173   6.18x   79,439    803,005
//!   wal_replay              17,532   153,184   8.74x     173   1,829  10.57x  950,528 12,484,864
//!   index_fold              36,788   367,186   9.98x      30      31   1.03x      337        367
//!   open_for_serving             2         2   1.00x       0       0      --        0          0
//!   TOTAL                   82,169   797,174   9.70x     243   2,045   8.42x 1,194,622 15,163,922
//!   residual                     0         0                  0       0                      0
//! ```
//!
//! 127 ms and 1,316 ms untraced, on a box at load 2.8. The count is the claim; the durations are
//! there for scale.
//!
//! A RESTORE IS NOT AN I/O PROBLEM. The large arm spends 1,316 ms and issues 2,045 file and
//! descriptor syscalls. Two thousand syscalls cannot cost a second. What it does instead is
//! allocate 797,174 times and 178 MB to bring back 20,000 records of 128 bytes: 39.9 allocations
//! and 8,940 allocated bytes per record recovered, against 2.56 MB of payload. Nothing here is
//! waiting on a disk.
//!
//! WHICH QUANTITY EACH PHASE TRACKS, held one at a time. Three arms: 2,000 x 128 B, then the SAME
//! 2,000 records at 1,280 B -- which moves the log 5.00x and nothing else -- then 20,000 x 128 B,
//! ten times the records at 1.30x the second arm's log:
//!
//! ```text
//!   allocations in           BASE    +5x bytes, =records    +10x records, =bytes
//!   manifest_and_base_index 17,587                 1.00x                   9.93x
//!   publish_and_seed        10,256                 1.00x                   9.94x
//!   wal_replay              17,521                 0.90x                   9.67x
//!   index_fold              36,784                 1.00x                   9.98x
//! ```
//!
//! EVERY PHASE TRACKS RECORDS AND NONE OF THEM TRACKS LOG BYTES. Five times the log at a fixed
//! record count moves no phase at all; ten times the records at the same log moves all four by
//! ten. The layer underneath measures the opposite way round -- a log WALK costs the log's bytes
//! -- and the difference is everything the engine does with a record after the walk hands it
//! over, which does not care how wide the value was. The one thing the wider arm does move is
//! replay's allocated BYTES, 5.28 MB -> 28.3 MB for 5.00x the log, which is the payload passing
//! through and nothing else.
//!
//! So the quantity that bounds a restart is RECORDS. A restart budget denominated in log bytes
//! would let a shard of narrow records run up an arbitrarily long recovery under a byte ceiling
//! it never reaches.
//!
//! WHAT IS SUPERLINEAR IS INHERITED, AND RESTORE PAYS IT WHOLE. The walk under replay re-reads
//! pieces from the front once per window:
//!
//! ```text
//!               log on disk   bytes replay read   read(2) calls   windows   share of syscalls
//!     2,000          524,288            950,528              73         1                 71%
//!    20,000        3,407,872         12,484,864           1,481         6                 89%
//!      ratio           6.50x             13.14x          20.29x
//! ```
//!
//! 6.50x the log gives 13.14x the bytes read and 20.29x the read calls -- 1.81x the log at the
//! small size and 3.66x at the large one. Restore neither amplifies it nor bounds it: it is the
//! only superlinear thing in a restore, it is 89% of the large restore's syscalls, and it is not
//! this file's code. What DOES bound it is that a restore is allocation-bound, so those syscalls
//! are 2,045 out of 1,316 ms.
//!
//! WHAT IS MATERIALISED WHOLE -- three whole-shard index images per restore:
//!
//!   * the pick decodes the winning manifest's embedded index image whole, 1,875,686 B at 20,000
//!     records, which it must: that image IS the checkpoint;
//!   * and it re-serialises EVERY manifest it lists, whole, to verify a checksum, in order to
//!     read four small fields off it -- another 1,875,622 B for one manifest, 3.48x that for a
//!     shard carrying six checkpoints of history;
//!   * and the fold at the end serialises the whole shard again to persist it, 1,268,705 B.
//!
//! About 5.0 MB of whole-shard index documents through serde to recover 2.56 MB of payload.
//!
//! AND 22% OF THE FOLD IS A WHOLE-SHARD SCAN THAT DECIDES TO DO NOTHING.
//! `promote_model_maps_to_bucket_index_authority` opens with
//! `collect_model_live_block_entries(shard)`, which materialises a vector of every live block
//! entry in the shard, and then tests whether any of them is missing from the bucket index. On
//! this path none ever is -- the replay above has already put them there -- so it returns false
//! and neither the ownership rebuild nor the secondary-view reconcile behind it runs at all.
//! Measured by removing the call: the fold drops from 36,788 to 28,783 allocations at 2,000
//! records and 367,186 to 287,167 at 20,000. Exactly 4.0 allocations per record the shard holds,
//! 22% of the fold and 10% of the whole restore, to answer a question whose answer is no.
//!
//! It is a precondition and not dead code -- when the index and the model maps HAVE diverged,
//! which is the #1822 shape, this is the check that notices. What is avoidable is the VECTOR: the
//! test is an `any()`, so the entries could be walked without being collected first. That call
//! lives in `engine/storage_bucket_internals.rs` and is reported rather than changed here.
//!
//! TWO THINGS THE PHASE NAMES DO NOT PREDICT, both in
//! `the_fold_tracks_the_store_and_the_replay_decodes_what_the_checkpoint_already_covers`:
//!
//!   * the fold is the LARGEST phase at both sizes, 45% of the restore, and it is proportional to
//!     the SHARD rather than to anything that was recovered -- nine times the tail moved it
//!     1.01x. It rebuilds every bucket over the whole routing range whether one record was
//!     replayed or all of them;
//!   * and dumping more often made the restart MORE expensive. The same 2,000-record store came
//!     back in 94,826 allocations having dumped 1,800 of its records and 69,465 having dumped
//!     200. A record costs 17.5 allocations to recover from a checkpoint and 5.5 to replay from
//!     the log, so a dump moves records from the cheap side of a restart to the expensive one.
//!
//! NOTHING IS FIXED HERE. The candidate that was written, measured and declined is priced in
//! `the_checkpoint_pick_reads_every_manifest_whole_to_use_one_of_them`.
#![allow(clippy::all)]
// Three of the six tests here only exist with `alloc-probe`, so the
// sizes and helpers they use go unread in an ordinary build.
#![allow(dead_code)]
use super::*;
use crate::engine::lifecycle::restore_phase_probe;

const SMALL: usize = 2_000;
const LARGE: usize = 20_000;
const NARROW: usize = 128;
const WIDE: usize = 1_280;
const PROBE_READS: usize = 32;

/// xorshift64*, seeded by index so runs repeat and no two records share a payload.
///
/// A repeated byte compresses, and a log built from one would hold a fraction of the bytes its
/// record count suggests -- which would quietly destroy the arm that holds RECORDS fixed and
/// varies BYTES, by making the two arms the same corpus.
fn incompressible(len: usize, seed: u64) -> Vec<u8> {
    let mut state = 0x2545_F491_4F6C_DD1D_u64
        ^ (len as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15)
        ^ seed.wrapping_mul(0xD1B5_4A32_D192_ED03);
    (0..len)
        .map(|_| {
            state ^= state >> 12;
            state ^= state << 25;
            state ^= state >> 27;
            state.wrapping_mul(0x2545_F491_4F6C_DD1D) as u8
        })
        .collect()
}

fn dir_bytes(root: &std::path::Path) -> u64 {
    let mut total = 0u64;
    let Ok(entries) = std::fs::read_dir(root) else {
        return 0;
    };
    for entry in entries.flatten() {
        let Ok(metadata) = entry.metadata() else {
            continue;
        };
        if metadata.is_dir() {
            total = total.saturating_add(dir_bytes(&entry.path()));
        } else {
            total = total.saturating_add(metadata.len());
        }
    }
    total
}

fn files_in(root: &std::path::Path) -> (usize, u64) {
    let Ok(entries) = std::fs::read_dir(root) else {
        return (0, 0);
    };
    let mut count = 0usize;
    let mut bytes = 0u64;
    for entry in entries.flatten() {
        let Ok(metadata) = entry.metadata() else {
            continue;
        };
        if metadata.is_file() {
            count += 1;
            bytes = bytes.saturating_add(metadata.len());
        }
    }
    (count, bytes)
}

/// What a shard that went away without unloading left on disk.
#[derive(Debug, Clone)]
struct Corpus {
    records: usize,
    value_bytes: usize,
    /// Records the last dump manifest covers. The rest are the WAL tail a restore must replay.
    dumped: usize,
    tail: usize,
    wal_bytes: u64,
    wal_pieces: usize,
    manifest_files: usize,
    manifest_bytes: u64,
    index_log_bytes: u64,
    manifest_wal_sequence: u64,
}

/// A shard that took `records` writes, dumped `dumps` manifests over its first `dumped` of them,
/// and then went away.
///
/// The engine is DROPPED rather than unloaded. Unloading materialises the base index, which is
/// exactly the durable checkpoint a crash does not leave behind -- a corpus that had one would
/// measure a tidy shutdown and never enter the recovery arm at all.
fn build_corpus(
    dir: &std::path::Path,
    records: usize,
    value_bytes: usize,
    dumps: usize,
    dumped: usize,
) -> Corpus {
    let index_dir = dir.join("indexes");
    let mut manifest_wal_sequence = 0u64;
    {
        let engine = TemporalEngine::with_local_dirs(
            64 * 1024 * 1024,
            dir.join("cache"),
            dir.join("pages"),
            index_dir.clone(),
        );
        engine.load_shard(1);
        // `dumps` checkpoints over the first `dumped` records, so a corpus can carry the manifest
        // history a shard that has been up for a while actually has. Only the LAST of them is the
        // checkpoint a load recovers from; the rest are what the pick has to read past.
        let mut taken = 0usize;
        while taken < dumps {
            seed(
                &engine,
                dumped * taken / dumps.max(1),
                dumped * (taken + 1) / dumps.max(1),
                value_bytes,
            );
            let manifest = engine
                .create_bucket_dump_manifest(1, Vec::<u32>::new())
                .expect("the corpus needs a durable dump manifest to recover from");
            manifest_wal_sequence = manifest.wal_sequence;
            taken += 1;
        }
        seed(&engine, dumped, records, value_bytes);
    }
    let (manifest_files, manifest_bytes) =
        files_in(&index_dir.join("slot-dumps").join("shard-1"));
    let (wal_pieces, wal_bytes) = files_in(&index_dir.join("wals"));
    Corpus {
        records,
        value_bytes,
        dumped,
        tail: records - dumped,
        wal_bytes,
        wal_pieces,
        manifest_files,
        manifest_bytes,
        index_log_bytes: dir_bytes(&index_dir.join("indexlogs")),
        manifest_wal_sequence,
    }
}

fn seed(engine: &TemporalEngine, from: usize, to: usize, value_bytes: usize) {
    let mut index = from;
    while index < to {
        let end = (index + 100).min(to);
        let mut commands = Vec::new();
        let mut cursor = index;
        while cursor < end {
            commands.push(Command::StringSet {
                key: format!("k-{cursor:08}"),
                value: incompressible(value_bytes, cursor as u64),
            });
            cursor += 1;
        }
        let response = engine.batch_execute(crate::types::BatchExecuteRequest {
            shard_id: 1,
            commands,
        });
        assert!(response.status.ok, "seed write failed: {:?}", response.status);
        index = end;
    }
}

/// What the restart cost.
#[derive(Debug, Clone)]
struct RestoreMeasurement {
    corpus: Corpus,
    cost: restore_phase_probe::RestoreCost,
    wal_window_walks: u64,
    wal_bytes_read: u64,
    manifest_listings: u64,
    manifest_file_reads: u64,
    manifest_bytes_read: u64,
    manifest_checksum_serializes: u64,
    manifest_checksum_bytes: u64,
    replayed_from: u64,
    readable_after: usize,
    wall_ms: u64,
}

impl RestoreMeasurement {
    #[allow(dead_code)]
    fn phase_allocs(&self, name: &str) -> u64 {
        self.cost.phase(name).allocs
    }
}

/// Restore the shard the corpus left behind, in this process, with every counter zeroed first.
fn restore(dir: &std::path::Path, corpus: Corpus) -> RestoreMeasurement {
    let engine = TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.join("cache-restore"),
        dir.join("pages"),
        dir.join("indexes"),
    );
    crate::engine::reset_bucket_dump_manifest_io_counts();
    let started = std::time::Instant::now();
    let response = engine.load_shard_with(LoadShardRequest {
        shard_id: 1,
        load_version: 1,
        local_node_id: None,
        shard_uri: String::new(),
        start_routing_bucket: 0,
        end_routing_bucket: u32::MAX,
        readonly: false,
        table_name: "t".to_string(),
    });
    let cost = restore_phase_probe::finish();
    let wall_ms = started.elapsed().as_millis() as u64;
    assert!(
        response.status.ok,
        "the restore under measurement must succeed, or every number below describes a failure: \
         {:?}",
        response.status
    );
    let manifest_counts = crate::engine::bucket_dump_manifest_io_counts();
    let wal = engine.write_ahead_log_store().raw_stats(1);
    // The restore is only worth counting if it brought the records back. The probe reads are
    // spread across the WHOLE key space, so a restore that recovered only the dumped half -- the
    // #1637 shape -- fails here instead of being reported as a cheap recovery.
    let mut readable = 0usize;
    let mut probe = 0usize;
    while probe < PROBE_READS {
        let index = probe * corpus.records / PROBE_READS;
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet {
                key: format!("k-{index:08}"),
            },
        });
        let recovered = matches!(
            &response.response,
            CommandResponse::Bytes { value }
                if value.as_ref().map(|bytes| bytes.len()) == Some(corpus.value_bytes)
        );
        if response.status.ok && recovered {
            readable += 1;
        }
        probe += 1;
    }
    RestoreMeasurement {
        corpus,
        cost,
        wal_window_walks: wal.scans,
        wal_bytes_read: wal.bytes_read,
        manifest_listings: manifest_counts.dir_listings,
        manifest_file_reads: manifest_counts.file_reads,
        manifest_bytes_read: manifest_counts.bytes_read,
        manifest_checksum_serializes: manifest_counts.checksum_serializes,
        manifest_checksum_bytes: manifest_counts.checksum_bytes,
        replayed_from: crate::engine::lifecycle::LAST_REPLAY_WATERMARK
            .load(std::sync::atomic::Ordering::SeqCst),
        readable_after: readable,
        wall_ms,
    }
}

fn measure(records: usize, value_bytes: usize) -> RestoreMeasurement {
    measure_with(records, value_bytes, 1, records / 2)
}

fn measure_with(
    records: usize,
    value_bytes: usize,
    dumps: usize,
    dumped: usize,
) -> RestoreMeasurement {
    let dir = tempfile::tempdir().expect("tempdir");
    let corpus = build_corpus(dir.path(), records, value_bytes, dumps, dumped);
    restore(dir.path(), corpus)
}

fn report(label: &str, measurement: &RestoreMeasurement) {
    let corpus = &measurement.corpus;
    println!(
        "\n{label}\n  corpus: {} records of {} B ({} dumped, {} left as a WAL tail) -- WAL {} B \
         in {} piece(s), {} manifest file(s) {} B, index-log {} B",
        corpus.records,
        corpus.value_bytes,
        corpus.dumped,
        corpus.tail,
        corpus.wal_bytes,
        corpus.wal_pieces,
        corpus.manifest_files,
        corpus.manifest_bytes,
        corpus.index_log_bytes
    );
    println!(
        "  replayed from wal_sequence {} (last manifest anchor {}), {} of {} probed keys \
         readable, wall {} ms",
        measurement.replayed_from,
        corpus.manifest_wal_sequence,
        measurement.readable_after,
        PROBE_READS,
        measurement.wall_ms
    );
    println!("  phase                       allocs     alloc_bytes        ms");
    for phase in &measurement.cost.phases {
        println!(
            "  {:<24} {:>10} {:>15} {:>9.1}",
            phase.phase,
            phase.allocs,
            phase.alloc_bytes,
            phase.nanos as f64 / 1e6
        );
    }
    println!(
        "  {:<24} {:>10} {:>15} {:>9.1}   <- every phase, summed",
        "ALL PHASES",
        measurement.cost.allocs(),
        measurement.cost.alloc_bytes(),
        measurement.cost.nanos() as f64 / 1e6
    );
    if !cfg!(feature = "alloc-probe") {
        println!("  (the two allocation columns read zero: built without `alloc-probe`)");
    }
    println!(
        "  WAL      window walks {} bytes_read {}",
        measurement.wal_window_walks, measurement.wal_bytes_read
    );
    println!(
        "  MANIFEST listings {} file_reads {} bytes_read {} checksum_serializes {} \
         checksum_bytes {}",
        measurement.manifest_listings,
        measurement.manifest_file_reads,
        measurement.manifest_bytes_read,
        measurement.manifest_checksum_serializes,
        measurement.manifest_checksum_bytes
    );
    println!(
        "  ENGINE LOCK over the index fold: {} write syscall(s) under it, {} after releasing it",
        measurement.cost.writes_under_engine_lock, measurement.cost.writes_after_engine_lock
    );
    println!(
        "  WALK     decoded {} records, {} of them ({:.0}%) already covered by the checkpoint and \
         dropped",
        measurement.cost.records_decoded,
        measurement.cost.records_behind_checkpoint,
        100.0 * measurement.cost.records_behind_checkpoint as f64
            / measurement.cost.records_decoded.max(1) as f64,
    );
}

/// THE WHOLE RESTORE, AT TWO CORPUS SIZES, BROKEN INTO ITS PHASES.
///
/// The phases sum to the whole by construction, so this reports a complete bill rather than a
/// selection from one. What it asserts is the two things that decide where an operator should
/// look: that the fold at the end is the biggest single phase at BOTH sizes, and that the whole
/// thing is flat per record rather than degrading as the shard grows.
#[cfg(feature = "alloc-probe")]
#[test]
fn what_bringing_a_shard_back_costs_at_two_corpus_sizes() {
    let small = measure(SMALL, NARROW);
    // BEFORE ANY NUMBER IS READ: the counting allocator is installed. Without it every
    // allocation column is a placeholder, and every ratio below would be zero against
    // zero -- which passes a `contains` band as readily as a real measurement does.
    assert!(
        small.cost.allocations_were_counted,
        "the counting allocator must be installed for this test to measure anything"
    );
    report("SMALL  (2,000 records)", &small);
    let large = measure(LARGE, NARROW);
    report("LARGE  (20,000 records)", &large);

    let ratio = large.cost.allocs() as f64 / small.cost.allocs().max(1) as f64;
    println!(
        "\n  10.00x the records: allocations {:.2}x, allocated bytes {:.2}x, log bytes read \
         {:.2}x, manifest bytes read {:.2}x",
        ratio,
        large.cost.alloc_bytes() as f64 / small.cost.alloc_bytes().max(1) as f64,
        large.wal_bytes_read as f64 / small.wal_bytes_read.max(1) as f64,
        large.manifest_bytes_read as f64 / small.manifest_bytes_read.max(1) as f64,
    );
    println!(
        "  per record recovered: {:.1} -> {:.1} allocations, {:.0} -> {:.0} allocated bytes, for \
         a {} B value",
        small.cost.allocs() as f64 / SMALL as f64,
        large.cost.allocs() as f64 / LARGE as f64,
        small.cost.alloc_bytes() as f64 / SMALL as f64,
        large.cost.alloc_bytes() as f64 / LARGE as f64,
        NARROW,
    );

    // NON-VACUOUS FIRST. A restore that recovered nothing, or that never entered the recovery
    // arm, would make every number above a description of an empty loop.
    assert!(
        small.replayed_from > 0 && small.replayed_from == small.corpus.manifest_wal_sequence,
        "the restore must have started from the dump manifest's anchor ({}), not from {}",
        small.corpus.manifest_wal_sequence,
        small.replayed_from
    );
    assert_eq!(
        (small.readable_after, large.readable_after),
        (PROBE_READS, PROBE_READS),
        "every probed key, spread across the whole key space, must read back after the restore"
    );

    // The phase rows account for the whole restore: a phase added later without a boundary shows
    // up here as a difference rather than vanishing from the bill.
    assert_eq!(
        small.cost.phases.len(),
        5,
        "a restore is five phases; found {:?}",
        small
            .cost
            .phases
            .iter()
            .map(|phase| phase.phase.clone())
            .collect::<Vec<_>>()
    );
    assert_eq!(
        small.cost.phases.iter().map(|phase| phase.allocs).sum::<u64>(),
        small.cost.allocs(),
        "the phase rows must sum to the whole restore"
    );

    // HALF ONE: the fold at the end is the largest phase, at BOTH sizes. It is also the phase
    // with almost no syscalls in it -- 30 and 31 -- so a restore that feels slow is not waiting
    // on the disk, it is rebuilding and re-serialising the whole shard.
    let biggest = |measurement: &RestoreMeasurement| {
        measurement
            .cost
            .phases
            .iter()
            .max_by_key(|phase| phase.allocs)
            .map(|phase| phase.phase.clone())
            .unwrap_or_default()
    };
    assert_eq!(
        (biggest(&small).as_str(), biggest(&large).as_str()),
        ("index_fold", "index_fold"),
        "the post-replay fold must be the largest phase of a restore at both corpus sizes"
    );

    // HALF TWO, and independent of the first: ten times the shard costs about ten times the
    // restore. Restore is flat per record -- it does not degrade as a store grows.
    assert!(
        (7.5..=12.5).contains(&ratio),
        "10.00x the records must cost about 10x the restore, not more and not less; it cost \
         {ratio:.2}x ({} -> {} allocations)",
        small.cost.allocs(),
        large.cost.allocs()
    );

    // HALF THREE: and what the fold costs, per record the SHARD holds, is the same number at
    // both sizes. That is the claim the two rows above turn into a rate, and it is the one a
    // change to any of the four whole-range passes inside the fold has to move.
    let fold_rate = |measurement: &RestoreMeasurement, records: usize| {
        measurement.phase_allocs("index_fold") as f64 / records as f64
    };
    println!(
        "  the fold costs {:.1} and {:.1} allocations per record the shard holds",
        fold_rate(&small, SMALL),
        fold_rate(&large, LARGE),
    );
    assert!(
        (16.5..=20.5).contains(&fold_rate(&small, SMALL))
            && (16.5..=20.5).contains(&fold_rate(&large, LARGE)),
        "the fold must cost the same per stored record at both sizes: {:.1} at {SMALL} records \
         and {:.1} at {LARGE}",
        fold_rate(&small, SMALL),
        fold_rate(&large, LARGE),
    );
}

/// WHICH QUANTITY EACH PHASE TRACKS, by holding one fixed and moving the other.
///
/// Three arms. BASE, then the same record count at ten times the value width -- which moves the
/// log and nothing else -- then ten times the records at about the same log. A phase that tracks
/// bytes moves in the second and not the third; a phase that tracks records does the opposite.
#[cfg(feature = "alloc-probe")]
#[test]
fn which_quantity_each_restore_phase_tracks() {
    let base = measure(SMALL, NARROW);
    // BEFORE ANY NUMBER IS READ: the counting allocator is installed. Without it every
    // allocation column is a placeholder, and every ratio below would be zero against
    // zero -- which passes a `contains` band as readily as a real measurement does.
    assert!(
        base.cost.allocations_were_counted,
        "the counting allocator must be installed for this test to measure anything"
    );
    report("BASE   2,000 x 128 B", &base);
    let wider = measure(SMALL, WIDE);
    report("WIDER  2,000 x 1,280 B -- more log bytes, the SAME records", &wider);
    let more = measure(LARGE, NARROW);
    report("MORE   20,000 x 128 B -- 10x the records, about the SAME log", &more);

    println!(
        "\n  quantity                    BASE         WIDER          MORE   WIDER/BASE  MORE/WIDER"
    );
    let row = |name: &str, a: u64, b: u64, c: u64| {
        println!(
            "  {:<22} {:>12} {:>13} {:>13} {:>12.2} {:>11.2}",
            name,
            a,
            b,
            c,
            b as f64 / a.max(1) as f64,
            c as f64 / b.max(1) as f64
        );
    };
    row("log bytes on disk", base.corpus.wal_bytes, wider.corpus.wal_bytes, more.corpus.wal_bytes);
    row("log bytes read", base.wal_bytes_read, wider.wal_bytes_read, more.wal_bytes_read);
    row("log window walks", base.wal_window_walks, wider.wal_window_walks, more.wal_window_walks);
    row(
        "manifest bytes read",
        base.manifest_bytes_read,
        wider.manifest_bytes_read,
        more.manifest_bytes_read,
    );
    row(
        "phase1 allocs",
        base.phase_allocs("manifest_and_base_index"),
        wider.phase_allocs("manifest_and_base_index"),
        more.phase_allocs("manifest_and_base_index"),
    );
    row(
        "phase2 allocs",
        base.phase_allocs("publish_and_seed"),
        wider.phase_allocs("publish_and_seed"),
        more.phase_allocs("publish_and_seed"),
    );
    row(
        "phase3 allocs",
        base.phase_allocs("wal_replay"),
        wider.phase_allocs("wal_replay"),
        more.phase_allocs("wal_replay"),
    );
    row(
        "phase4 allocs",
        base.phase_allocs("index_fold"),
        wider.phase_allocs("index_fold"),
        more.phase_allocs("index_fold"),
    );
    row(
        "phase3 alloc BYTES",
        base.cost.phase("wal_replay").alloc_bytes,
        wider.cost.phase("wal_replay").alloc_bytes,
        more.cost.phase("wal_replay").alloc_bytes,
    );

    // The control has to have controlled something. Both of these are what makes the two ratio
    // columns mean opposite things, and a fixture where the WIDER arm failed to widen would make
    // the whole test agree with itself for no reason.
    assert_eq!(
        (base.corpus.records, wider.corpus.records),
        (SMALL, SMALL),
        "the WIDER arm holds the record count fixed"
    );
    assert!(
        wider.corpus.wal_bytes >= base.corpus.wal_bytes * 3,
        "the WIDER arm must actually carry far more log bytes at the same record count: {} B \
         against {} B",
        wider.corpus.wal_bytes,
        base.corpus.wal_bytes
    );

    // HALF ONE, ASSERTED FIRST: five times the log at the SAME record count moves no phase's
    // allocation count. Not one of the four is within reach of the 5.00x the log grew by.
    let widened = |name: &str| {
        wider.phase_allocs(name) as f64 / base.phase_allocs(name).max(1) as f64
    };
    let bytes_ratios = [
        ("manifest_and_base_index", widened("manifest_and_base_index")),
        ("publish_and_seed", widened("publish_and_seed")),
        ("wal_replay", widened("wal_replay")),
        ("index_fold", widened("index_fold")),
    ];
    for (name, ratio) in bytes_ratios {
        assert!(
            (0.7..=1.4).contains(&ratio),
            "{name} must not track log bytes: it moved {ratio:.2}x when the log grew {:.2}x at a \
             fixed record count",
            wider.corpus.wal_bytes as f64 / base.corpus.wal_bytes.max(1) as f64
        );
    }
    // And what DOES move with the bytes is the payload passing through replay, which is the one
    // thing in this table that should.
    assert!(
        wider.cost.phase("wal_replay").alloc_bytes
            >= base.cost.phase("wal_replay").alloc_bytes * 3,
        "replay's allocated BYTES must follow the log: {} B against {} B",
        wider.cost.phase("wal_replay").alloc_bytes,
        base.cost.phase("wal_replay").alloc_bytes
    );

    // HALF TWO, independent of the first: ten times the records at about the same log moves every
    // phase by about ten.
    let more_records = |name: &str| {
        more.phase_allocs(name) as f64 / wider.phase_allocs(name).max(1) as f64
    };
    for name in [
        "manifest_and_base_index",
        "publish_and_seed",
        "wal_replay",
        "index_fold",
    ] {
        let ratio = more_records(name);
        assert!(
            (7.0..=13.0).contains(&ratio),
            "{name} must track records: it moved {ratio:.2}x for 10.00x the records at {:.2}x \
             the log bytes",
            more.corpus.wal_bytes as f64 / wider.corpus.wal_bytes.max(1) as f64
        );
    }
}

/// WHAT THE FOLD IS PROPORTIONAL TO -- the STORE -- AND WHAT REPLAY COSTS WHEN THERE IS ALMOST
/// NOTHING LEFT TO REPLAY.
///
/// Two corpora of the same size whose dumped/replayed split differs by nine times, so the store
/// is held fixed and the tail is the only thing that moves. Neither half of this came out where
/// the phase names suggest it would.
///
/// THE FOLD DOES NOT MOVE. `promote_model_maps_to_bucket_index_authority` and
/// `rebuild_bucket_first_index` are both called over the WHOLE routing range at the end of
/// replay, so the fold rebuilds every bucket in the shard whether one record was replayed or all
/// of them. Nine times the tail, 1.01x the fold. That is why the fold is the largest phase of a
/// restore: it is the only one proportional to the shard rather than to the work.
///
/// AND REPLAY IS MOSTLY NOT THE TAIL EITHER. Nine times the tail costs 1.67x the replay, because
/// the walk starts at the first log piece that could hold anything after the checkpoint and
/// decodes every record in it -- integrity envelope and all -- and only then drops the ones the
/// checkpoint already covers. Both arms decoded 20 log records; the short-tail arm threw 18 of
/// them away. Replaying 200 records cost 60% of what replaying 1,800 cost.
///
/// The skip is by PIECE, which is the right granularity to skip at -- a finer one would mean
/// trusting a record's claimed sequence before reading it -- so what this measures is the price
/// of that choice at its worst: a shard that dumps often has a short tail every time.
///
/// AND SO DUMPING MORE OFTEN MADE THE RESTART MORE EXPENSIVE, not less. The same 2,000-record
/// store, restored:
///
/// ```text
///                        checkpoint   replay    fold    TOTAL
///   1,800 records dumped     31,557   13,143  36,666   94,826 allocations, 148 ms
///     200 records dumped      3,615   21,890  36,906   69,465 allocations,  84 ms
/// ```
///
/// 17.5 allocations per record recovered from the checkpoint against 5.5 per record replayed
/// from the log: pulling a record out of a manifest's embedded index image costs about three
/// times what re-executing its write costs. A dump moves records from the cheap side of a
/// restart to the expensive one, and buys back only the part of replay that is per-record --
/// which, at a short tail, is the smaller part.
#[cfg(feature = "alloc-probe")]
#[test]
fn the_fold_tracks_the_store_and_the_replay_decodes_what_the_checkpoint_already_covers() {
    let light = measure_with(SMALL, NARROW, 1, SMALL * 9 / 10);
    // BEFORE ANY NUMBER IS READ: the counting allocator is installed. Without it every
    // allocation column is a placeholder, and every ratio below would be zero against
    // zero -- which passes a `contains` band as readily as a real measurement does.
    assert!(
        light.cost.allocations_were_counted,
        "the counting allocator must be installed for this test to measure anything"
    );
    report("TAIL-LIGHT  2,000 records, 200 left to replay", &light);
    let heavy = measure_with(SMALL, NARROW, 1, SMALL / 10);
    report("TAIL-HEAVY  2,000 records, 1,800 left to replay", &heavy);
    println!(
        "\n  tail {} -> {} records ({:.2}x) at an unchanged store of {} records:\n    \
         replay allocs {} -> {} ({:.2}x)   fold allocs {} -> {} ({:.2}x)\n    \
         records the walk decoded {} -> {}, of which already durable {} -> {}",
        light.corpus.tail,
        heavy.corpus.tail,
        heavy.corpus.tail as f64 / light.corpus.tail.max(1) as f64,
        SMALL,
        light.phase_allocs("wal_replay"),
        heavy.phase_allocs("wal_replay"),
        heavy.phase_allocs("wal_replay") as f64 / light.phase_allocs("wal_replay").max(1) as f64,
        light.phase_allocs("index_fold"),
        heavy.phase_allocs("index_fold"),
        heavy.phase_allocs("index_fold") as f64 / light.phase_allocs("index_fold").max(1) as f64,
        light.cost.records_decoded,
        heavy.cost.records_decoded,
        light.cost.records_behind_checkpoint,
        heavy.cost.records_behind_checkpoint,
    );

    assert_eq!(
        (light.corpus.records, heavy.corpus.records),
        (SMALL, SMALL),
        "both arms must hold the same number of records, or the store is not the thing held fixed"
    );
    assert_eq!(
        (light.readable_after, heavy.readable_after),
        (PROBE_READS, PROBE_READS),
        "both arms must have recovered the whole key space"
    );

    // HALF ONE, FIRST, and it is what makes the rest non-vacuous: the two arms really do differ
    // by nine times in what there was to replay.
    assert!(
        heavy.corpus.tail >= light.corpus.tail * 8,
        "the fixture must vary the tail: {} records against {}",
        heavy.corpus.tail,
        light.corpus.tail
    );

    // HALF TWO: the fold does not notice. It rebuilds the shard, and the shard did not change.
    let fold_ratio =
        heavy.phase_allocs("index_fold") as f64 / light.phase_allocs("index_fold").max(1) as f64;
    assert!(
        (0.8..=1.25).contains(&fold_ratio),
        "the fold must track the STORE and not the tail: it moved {fold_ratio:.2}x for a nine \
         times longer tail ({} -> {} allocations)",
        light.phase_allocs("index_fold"),
        heavy.phase_allocs("index_fold"),
    );

    // HALF THREE: and replay barely notices either, because most of what it decodes is already
    // durable. Asserted on the census rather than on the ratio, so it names the mechanism.
    assert!(
        light.cost.records_behind_checkpoint >= light.cost.records_decoded * 3 / 4,
        "at a short tail most of what the walk decodes must already be covered by the \
         checkpoint: {} of {} records",
        light.cost.records_behind_checkpoint,
        light.cost.records_decoded,
    );
    assert!(
        light.cost.records_decoded >= heavy.cost.records_decoded * 3 / 4,
        "and the walk decodes about the same number of records either way -- {} against {} -- \
         which is why a nine times shorter tail is not a nine times cheaper replay",
        light.cost.records_decoded,
        heavy.cost.records_decoded,
    );

    // HALF FOUR: and so the shard that had dumped NINE TIMES MORE of itself was the more
    // expensive of the two to bring back. Nothing about the phase names predicts that.
    println!(
        "    per record: {:.1} allocations to recover one from the checkpoint, {:.1} to replay \
         one from the log",
        light.phase_allocs("manifest_and_base_index") as f64 / light.corpus.dumped.max(1) as f64,
        (heavy.phase_allocs("wal_replay") as f64 - light.phase_allocs("wal_replay") as f64)
            / (heavy.corpus.tail - light.corpus.tail).max(1) as f64,
    );
    assert!(
        light.cost.allocs() > heavy.cost.allocs() * 11 / 10,
        "the more-dumped shard must be the more expensive restart, which is the whole point: {} \
         allocations having dumped {} records, against {} having dumped {}",
        light.cost.allocs(),
        light.corpus.dumped,
        heavy.cost.allocs(),
        heavy.corpus.dumped,
    );
}

/// WHAT THE CHECKPOINT PICK COSTS PER MANIFEST ON DISK -- REPORTED, NOT FIXED.
///
/// `durable_recovery_bucket_dump_manifest_at` lists every dump manifest for the shard and takes a
/// maximum over `wal_sequence`. Listing one means reading the file whole, parsing it whole, and
/// re-serialising it whole to check its checksum -- and a manifest embeds a whole-shard index
/// image, so each of those is proportional to the STORE. Every manifest but the winner is read,
/// parsed, re-serialised, hashed and dropped.
///
/// THE CANDIDATE FIX, PRICED AND DECLINED. Deserialise a cheap three-integer header from each
/// file, take the maximum on that, and read + verify only the winner whole. It was written and
/// applied as a change and this test re-run against it, on the six-checkpoint arm:
///
/// ```text
///                                       in the tree      candidate
///   manifests fully parsed + hashed               6              1
///   bytes re-serialised for a checksum      571,570        165,129     3.46x less
///   checkpoint phase, allocated bytes       7.23 MB        6.08 MB
///   checkpoint phase, wall                   78.6 ms        54.2 ms
///   bytes read off disk                     571,954        737,147     MORE, see below
/// ```
///
/// The bytes read go UP because the winner is read twice, once for its header and once whole;
/// keeping the first read's buffer would undo that, and would not change the decision.
///
/// IT IS NOT TAKEN. The checksum is what says the document on disk is the document that was
/// written. A pick made on a field read BEFORE that check is a pick made on an unverified number,
/// and what it selects is the durable checkpoint an entire shard is about to be rebuilt from. A
/// flipped byte inside `wal_sequence` wins the maximum by construction -- corruption can only
/// move the number up or down, and up wins -- so the one manifest a bit-rotted file is most
/// likely to be chosen over is the good one. The load would then verify the winner, find it
/// intact, and recover from a checkpoint chosen by the damage. Trusting a name ahead of the
/// header it belongs to is the defect #1762 took out of the walking reader; this would put it
/// back somewhere that decides what a whole shard becomes.
///
/// The cost is real, it is bounded by the retained checkpoint history, and the cheap place to
/// spend the same effort is retaining fewer of them -- which is a durability decision and not a
/// tuning one.
#[test]
fn the_checkpoint_pick_reads_every_manifest_whole_to_use_one_of_them() {
    let one = measure_with(SMALL, NARROW, 1, SMALL / 2);
    report("ONE CHECKPOINT", &one);
    let six = measure_with(SMALL, NARROW, 6, SMALL / 2);
    report("SIX CHECKPOINTS -- the same store, six dumps of history", &six);
    println!(
        "\n  manifests on disk {} -> {};  read by the pick {} B -> {} B ({:.2}x);  \
         re-serialised to check a checksum {} B -> {} B ({:.2}x);  used by the pick: one index \
         image, plus one integer from each of the others",
        one.corpus.manifest_files,
        six.corpus.manifest_files,
        one.manifest_bytes_read,
        six.manifest_bytes_read,
        six.manifest_bytes_read as f64 / one.manifest_bytes_read.max(1) as f64,
        one.manifest_checksum_bytes,
        six.manifest_checksum_bytes,
        six.manifest_checksum_bytes as f64 / one.manifest_checksum_bytes.max(1) as f64,
    );

    // HALF ONE, FIRST: one manifest is read once, which is what makes the six below a multiple of
    // something rather than a number on its own.
    assert_eq!(
        (one.corpus.manifest_files, one.manifest_file_reads, one.manifest_checksum_serializes),
        (1, 1, 1),
        "a shard with one checkpoint must have it read and checksummed exactly once"
    );

    // HALF TWO: six manifests are ALL read and ALL re-serialised, and the pick returns one.
    assert_eq!(
        (six.corpus.manifest_files, six.manifest_file_reads, six.manifest_checksum_serializes),
        (6, 6, 6),
        "every manifest on disk is read whole and re-serialised whole to pick one of them"
    );
    assert_eq!(
        six.manifest_bytes_read, six.corpus.manifest_bytes,
        "the pick reads every byte of every manifest file on disk"
    );
    assert!(
        six.manifest_checksum_bytes >= six.manifest_bytes_read * 9 / 10,
        "and puts all of them back through serde to verify a checksum: {} B re-serialised \
         against {} B read",
        six.manifest_checksum_bytes,
        six.manifest_bytes_read
    );
    // And having read six, it recovers from one -- the one carrying the highest WAL anchor.
    assert_eq!(
        six.replayed_from, six.corpus.manifest_wal_sequence,
        "the load recovers from the last checkpoint's anchor, so the other five were read for \
         one integer each"
    );
}

/// WHAT THE INDEX FOLD HOLDS THE ENGINE LOCK OVER, AND WHAT IT DOES NOT.
///
/// The fold rebuilds the bucket index and serialises the whole shard under `shards.write()`, then
/// the durable write of those bytes happens after the block scope ends. That the write is outside
/// is not visible at the call site -- the lock is a block and the persist is the next statement --
/// so it is counted rather than argued about. Every writer on this shard waits behind that lock,
/// and a durable barrier taken under it would make a restore block the engine for the length of a
/// disk write rather than the length of a memcpy.
#[test]
fn the_index_fold_writes_the_shard_image_outside_the_engine_lock() {
    let measurement = measure(SMALL, NARROW);
    report("FOLD", &measurement);
    // HALF ONE, FIRST, so a mutant that destroys it cannot hide behind the zero below: the fold
    // did write the rebuilt image somewhere. A restore that wrote nothing at all would satisfy
    // "nothing under the lock" for the wrong reason.
    assert!(
        measurement.cost.writes_after_engine_lock > 0,
        "vacuous otherwise: the fold must write the rebuilt index image, it issued {} write \
         syscalls after releasing the engine lock",
        measurement.cost.writes_after_engine_lock
    );
    // HALF TWO.
    assert_eq!(
        measurement.cost.writes_under_engine_lock, 0,
        "the index fold must not write to disk while it holds the engine write lock"
    );
}

/// WHAT THE WALK DECODES THAT THE CHECKPOINT ALREADY COVERS -- counted, no allocator needed.
///
/// The half of `the_fold_tracks_the_store_and_the_replay_decodes_what_the_checkpoint_already_covers`
/// that is a census of records rather than a comparison of costs, so the ordinary gate runs it.
/// The walk starts at the first log piece that could hold anything past the checkpoint and
/// decodes every record in that piece -- integrity envelope and all -- before the sequence test
/// drops the ones already durable. A shard that dumps often has a short tail every time, and
/// pays for the whole piece every time.
#[test]
fn the_replay_decodes_records_the_checkpoint_already_covers() {
    let light = measure_with(SMALL, NARROW, 1, SMALL * 9 / 10);
    report("TAIL-LIGHT  2,000 records, 200 left to replay", &light);
    let heavy = measure_with(SMALL, NARROW, 1, SMALL / 10);
    report("TAIL-HEAVY  2,000 records, 1,800 left to replay", &heavy);
    println!(
        "\n  records the walk decoded {} -> {}, of which the checkpoint already covered {} -> {}",
        light.cost.records_decoded,
        heavy.cost.records_decoded,
        light.cost.records_behind_checkpoint,
        heavy.cost.records_behind_checkpoint,
    );

    // NON-VACUOUS FIRST: the two arms really do differ by nine times in what there was to
    // replay, and both recovered the whole key space.
    assert!(
        heavy.corpus.tail >= light.corpus.tail * 8,
        "the fixture must vary the tail: {} records against {}",
        heavy.corpus.tail,
        light.corpus.tail
    );
    assert_eq!(
        (light.readable_after, heavy.readable_after),
        (PROBE_READS, PROBE_READS),
        "both arms must have recovered the whole key space"
    );

    // HALF ONE: at a short tail, most of what the walk decodes is already durable.
    assert!(
        light.cost.records_behind_checkpoint >= light.cost.records_decoded * 3 / 4,
        "at a short tail most of what the walk decodes must already be covered by the \
         checkpoint: {} of {} records",
        light.cost.records_behind_checkpoint,
        light.cost.records_decoded,
    );

    // HALF TWO: and the walk decodes about the same number either way, which is why a nine times
    // shorter tail is not a nine times cheaper replay.
    assert!(
        light.cost.records_decoded >= heavy.cost.records_decoded * 3 / 4,
        "the walk must decode about the same number of records either way: {} against {}",
        light.cost.records_decoded,
        heavy.cost.records_decoded,
    );
}
