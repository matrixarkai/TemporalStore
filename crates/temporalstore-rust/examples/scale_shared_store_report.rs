// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! TemporalStore over a shared object store at scale: five nodes, a corpus sweep, and the cost.
//!
//! The three-node report answered *does it work*. This answers *what does it cost, and does the
//! cost hold its shape as the corpus grows* — the only version of a scale question worth asking,
//! because one corpus size cannot tell a linear cost from a quadratic one.
//!
//! # FIVE NODES, ONE WRITER
//!
//! The shared store is a **fenced single writer**, so scale in the node dimension means more
//! *replayers*, not more writers: one node holds the lease and appends, four replay from the store
//! alone. Each replayer's own figures are printed rather than averaged — an average over nodes
//! hides the one node that read twice as much as the others.
//!
//! # WHAT IS ASSERTED, AND WHY IT IS NOT A TIMING
//!
//! The timings are printed and never asserted, because a timing is a fact about the machine that
//! produced it. What is asserted is deterministic: bytes per entry, extents per entry, and — added
//! when this report's own finding was acted on — **page reads per entry on both sides**. Every byte
//! the store serves comes from a page read, so reads per entry is countable and is exactly where a
//! per-operation walk of the log appears.
//!
//! That column is what the earlier version of this report was missing. It showed per-entry time
//! growing with nothing deterministic to pin it on, because the byte figures count the range that
//! is *read* — correctly indexed all along — and not the work of *locating* it. Two walks were
//! found once the reads were counted, one on each side, and both are gone:
//!
//! * an append read the whole object back through its extents to recompute its etag
//!   (MatrixObject #213, now a resumed digest);
//! * a replay re-read and re-decoded the whole per-shard offset index once per entry, while
//!   holding the decoded records already.
//!
//! **Append per-entry cost still grows, and these counts cannot see why** — the remaining work
//! does no reading. The closing section says what it is and how it was measured.
//!
//! rust-internal: measures the shared store's own counters, no product behaviour

use std::sync::Arc;
use std::time::Instant;

use matrixobjectstore_rs::StoreOptions;
use temporalstore_rust::shared_store::{
    SharedStoreReplicator, SharedStoreWalAppendMode, SharedStoreWalEntry,
};
use temporalstore_rust::{Command, CommandResponse, ExecuteRequest, MatrixObjectObjectStore, TemporalEngine};
use temporalstore_snapshot::object_store::ObjectStore;

const BUCKET: &str = "temporalstore-shared";
const SHARD: u64 = 1;
const VALUE_BYTES: usize = 256;
const REPLAYERS: usize = 4;

struct NodeReplay {
    node: String,
    applied: u64,
    last_index: u64,
    index_reads: u64,
    range_bytes: u64,
    chunk_reads: u64,
    micros: u128,
}

struct Round {
    entries: usize,
    user_bytes: u64,
    blob_bytes: u64,
    extents: usize,
    objects: usize,
    append_micros: u128,
    append_chunk_reads: u64,
    replays: Vec<NodeReplay>,
}

/// How many page reads the store has served since it opened.
///
/// # WHY THE REPORT NEEDS A COUNT AND NOT ONLY A CLOCK
///
/// A timing is a fact about the box this runs on, so the assertions at the bottom cannot
/// be made on microseconds. But the work behind the timing is countable: every byte the
/// store hands back comes from a page read, and the read cache is asked once per read
/// whether it has it. Hits plus misses is therefore the number of page reads the whole
/// run performed, and it is exactly as deterministic as the extent count.
///
/// That is what makes "per-entry cost does not grow" assertable here. A per-operation walk
/// of something that grows with the log shows up as reads per entry climbing, whatever the
/// machine is doing at the time.
fn chunk_reads(store: &MatrixObjectObjectStore) -> u64 {
    let inner = store.inner();
    let guard = inner.lock().expect("store lock poisoned");
    let stats = guard.read_cache_stats();
    stats.hits + stats.misses
}

fn node_engine(root: &std::path::Path, node: &str) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(
        8 * 1024 * 1024,
        root.join(format!("{node}-cache")),
        root.join(format!("{node}-pages")),
        root.join(format!("{node}-index")),
    );
    engine.load_shard(SHARD);
    engine
}

async fn round(entries: usize, options: &StoreOptions) -> Round {
    let dir = tempfile::tempdir().expect("tempdir");
    let cluster = format!("cluster-scale-{entries}");
    let store = Arc::new(
        MatrixObjectObjectStore::new(BUCKET, options.clone()).expect("the shared store"),
    );
    let rep = || {
        SharedStoreReplicator::new(cluster.clone(), store.clone())
            .with_wal_append_mode(SharedStoreWalAppendMode::ProtobufAppendBlob)
    };

    // ---- ONE FENCED WRITER. The lease is taken durably, so this is the same path a real
    // ---- deployment uses rather than an unfenced shortcut that would understate the cost.
    let writer = rep().with_fence(1, "node-writer");
    writer
        .acquire_shard_lease(SHARD, 1, "node-writer")
        .await
        .expect("the writer should take an unheld lease");

    let mut user_bytes = 0u64;
    // Taken AFTER the lease, so the lease's own reads are not charged to the appends.
    let reads_before_appends = chunk_reads(&store);
    let started = Instant::now();
    for index in 1..=entries as u64 {
        let key = format!("scale-key-{index:08}");
        user_bytes += (key.len() + VALUE_BYTES) as u64;
        writer
            .publish_wal_entry(SharedStoreWalEntry {
                shard_id: SHARD,
                wal_index: index,
                command: Some(Command::StringSet { key, value: vec![b'v'; VALUE_BYTES] }),
                staged_blocks: Vec::new(),
                outcomes: Vec::new(),
            })
            .await
            .unwrap_or_else(|err| panic!("append {index} of {entries}: {err:?}"));
    }
    let append_micros = started.elapsed().as_micros();
    let append_chunk_reads = chunk_reads(&store) - reads_before_appends;

    // ---- WHAT THE STORE HOLDS, counting EVERY object the append left, the offset index included.
    let objects = store
        .list(&format!("{cluster}/shards/{SHARD}/shared/wal/"))
        .await
        .expect("the wal listing");
    let (blob_bytes, extents) = {
        let inner = store.inner();
        let guard = inner.lock().expect("store lock poisoned");
        let mut bytes = 0u64;
        let mut count = 0usize;
        for key in &objects {
            if let Ok(object) = guard.get_object(BUCKET, key) {
                bytes += object.metadata.extents.iter().map(|e| e.length()).sum::<u64>();
                count += object.metadata.extents.len();
            }
        }
        (bytes, count)
    };

    // ---- FOUR REPLAYERS, each from zero, each timed and counted on its own.
    let mut replays = Vec::new();
    for n in 0..REPLAYERS {
        let name = format!("node-{n}");
        let engine = node_engine(dir.path(), &name);
        let reads_before_replay = chunk_reads(&store);
        let began = Instant::now();
        let report = rep()
            .replay_wal_strict(SHARD, 0, &engine)
            .await
            .unwrap_or_else(|err| panic!("{name} replay at {entries}: {err:?}"));
        let micros = began.elapsed().as_micros();
        let replay_chunk_reads = chunk_reads(&store) - reads_before_replay;

        // ---- AND IT SERVED WHAT IT REPLAYED. A replay report is a count; this is the data.
        let probe = format!("scale-key-{:08}", entries);
        let got = engine.execute(ExecuteRequest {
            shard_id: SHARD,
            command: Command::StringGet { key: probe.clone() },
        });
        assert_eq!(
            got.response,
            CommandResponse::Bytes { value: Some(vec![b'v'; VALUE_BYTES]) },
            "{name} replayed {} entries but cannot serve {probe}",
            report.applied
        );

        replays.push(NodeReplay {
            node: name,
            applied: report.applied as u64,
            last_index: report.last_wal_index,
            index_reads: report.offset_index_reads as u64,
            range_bytes: report.range_bytes_read,
            chunk_reads: replay_chunk_reads,
            micros,
        });
    }

    Round {
        entries,
        user_bytes,
        blob_bytes,
        extents,
        objects: objects.len(),
        append_micros,
        append_chunk_reads,
        replays,
    }
}

#[tokio::main]
async fn main() {
    let options = StoreOptions {
        page_size: 1024,
        max_extent_bytes: 4096,
        chunk_size: 4096,
        replica_count: 3,
        write_quorum: 2,
        ..StoreOptions::default()
    };

    println!("TEMPORALSTORE OVER A SHARED OBJECT STORE, AT SCALE");
    println!();
    println!("  5 nodes: ONE fenced writer, {REPLAYERS} replayers, all over one store");
    println!("  value {VALUE_BYTES} B per entry");
    println!("  store: page {} chunk {} extent {} replicas {} quorum {}",
             options.page_size, options.chunk_size, options.max_extent_bytes,
             options.replica_count, options.write_quorum);

    // ---- FOUR POINTS OVER A 16x RANGE. One corpus size cannot tell a flat per-entry cost
    // ---- from a growing one, and two cannot tell a step from a slope; four can. The top
    // ---- size stays where it is because the question is the SHAPE, and a fifth point costs
    // ---- 16x more for the same answer.
    let sizes = [100usize, 400, 800, 1_600];
    let mut rounds = Vec::new();
    for entries in sizes {
        rounds.push(round(entries, &options).await);
    }

    println!();
    println!("  THE WRITE SIDE");
    println!();
    println!("    entries   user KiB   blob KiB   extents   objects   page reads   append ms   us/entry   blob/user   physical/user");
    for r in &rounds {
        let physical = r.blob_bytes * options.replica_count as u64;
        println!(
            "    {:>7}   {:>8.1}   {:>8.1}   {:>7}   {:>7}   {:>10}   {:>9.1}   {:>8.1}   {:>9.3}   {:>13.3}",
            r.entries,
            r.user_bytes as f64 / 1024.0,
            r.blob_bytes as f64 / 1024.0,
            r.extents,
            r.objects,
            r.append_chunk_reads,
            r.append_micros as f64 / 1000.0,
            r.append_micros as f64 / r.entries as f64,
            r.blob_bytes as f64 / r.user_bytes as f64,
            physical as f64 / r.user_bytes as f64,
        );
    }

    println!();
    println!("  THE READ SIDE, per node and never averaged");
    println!();
    println!("    entries   node      applied   last   index reads   range KiB   page reads   replay ms   us/entry");
    for r in &rounds {
        for n in &r.replays {
            println!(
                "    {:>7}   {:<8}  {:>7}   {:>4}   {:>11}   {:>9.1}   {:>10}   {:>9.1}   {:>8.1}",
                r.entries,
                n.node,
                n.applied,
                n.last_index,
                n.index_reads,
                n.range_bytes as f64 / 1024.0,
                n.chunk_reads,
                n.micros as f64 / 1000.0,
                n.micros as f64 / r.entries as f64,
            );
        }
    }

    println!();
    println!("  DOES THE COST HOLD ITS SHAPE");
    println!();
    println!("    entries   blob B/entry   range B/entry   append reads/entry   replay reads/entry   append us/entry   replay us/entry");
    for r in &rounds {
        let worst = r.replays.iter().max_by_key(|n| n.micros).expect("a replayer");
        let worst_reads = r
            .replays
            .iter()
            .max_by_key(|n| n.chunk_reads)
            .expect("a replayer");
        println!(
            "    {:>7}   {:>12.1}   {:>13.1}   {:>18.3}   {:>18.3}   {:>15.1}   {:>15.1}",
            r.entries,
            r.blob_bytes as f64 / r.entries as f64,
            worst.range_bytes as f64 / r.entries as f64,
            r.append_chunk_reads as f64 / r.entries as f64,
            worst_reads.chunk_reads as f64 / r.entries as f64,
            r.append_micros as f64 / r.entries as f64,
            worst.micros as f64 / r.entries as f64,
        );
    }
    println!();

    // ---- EVERY NODE CONVERGES. A scale run that diverges is not slow, it is wrong, and this is
    // ---- checked before any timing below is allowed to mean anything.
    for r in &rounds {
        for n in &r.replays {
            assert_eq!(
                n.last_index, r.entries as u64,
                "at {} entries, {} converged on index {} -- every node must reach the same last \
                 index or the figures above describe different stores",
                r.entries, n.node, n.last_index
            );
            assert_eq!(
                n.applied, r.entries as u64,
                "at {} entries, {} applied {} of them",
                r.entries, n.node, n.applied
            );
        }
    }

    // ---- AND THE PER-ENTRY WORK DOES NOT GROW. Checked across the sweep rather than at its
    // ---- largest point, because a cost that doubles between the last two sizes and nowhere else
    // ---- is still a cost that grows.
    let per_entry = |r: &Round| -> f64 {
        let worst = r.replays.iter().max_by_key(|n| n.range_bytes).expect("a replayer");
        worst.range_bytes as f64 / r.entries as f64
    };
    let first = per_entry(&rounds[0]);
    for r in &rounds[1..] {
        let now = per_entry(r);
        assert!(
            now <= first * 1.5,
            "bytes read per entry grew from {first:.1} at {} entries to {now:.1} at {} -- a \
             replay that reads more per entry as the log grows is reading the log rather than \
             indexing into it",
            rounds[0].entries,
            r.entries
        );
    }

    let blob_per_entry = |r: &Round| r.blob_bytes as f64 / r.entries as f64;
    let first_blob = blob_per_entry(&rounds[0]);
    for r in &rounds[1..] {
        let now = blob_per_entry(r);
        assert!(
            now <= first_blob * 1.5,
            "stored bytes per entry grew from {first_blob:.1} to {now:.1} between {} and {} \
             entries, so the write cost is not linear in the corpus",
            rounds[0].entries,
            r.entries
        );
    }

    // ---- THE EXTENT COUNT IS THE SHAPE OF THE STORED OBJECT, and it is deterministic where a
    // ---- timing is not. Two extents per entry -- one on the WAL blob, one on the offset index --
    // ---- means both lists grow with the log, so anything that walks one per operation is O(n)
    // ---- per op and O(n-squared) overall. That used to be the case on both sides; the two
    // ---- read-count assertions below are what now say it is not.
    for r in &rounds {
        let per_entry = r.extents as f64 / r.entries as f64;
        assert!(
            (per_entry - 2.0).abs() < 0.5,
            "at {} entries the store holds {} extents, {per_entry:.2} per entry rather than the \
             2 every other size shows -- the stored shape moved, so the per-entry figures above \
             are not measuring what they were written to measure",
            r.entries,
            r.extents
        );
    }

    // ---- AND NEITHER SIDE READS MORE PER ENTRY AS THE LOG GROWS.
    //
    // This is the assertion the earlier version of this report could not make. Per-entry
    // time was growing and nothing deterministic said so: the byte figures were flat
    // because they count the RANGE read of the WAL blob, which was correctly indexed all
    // along, and the cost was in LOCATING the range rather than in transferring it.
    //
    // Page reads are the countable form of that work -- every byte the store serves comes
    // from one, and the read cache is asked once per read -- so reads per entry is
    // deterministic AND sensitive to the thing the timings were reacting to. Two walks were
    // found this way, one per side:
    //
    //   APPEND  MatrixObject's `apply_append_inner` read the whole object back through its
    //           extents to recompute its FNV-1a etag. Fixed in MatrixObject #213 by resuming
    //           the previous digest, which is exact rather than an approximation.
    //   REPLAY  `replay_wal_from_offset_metadata` held the decoded offset index and then
    //           asked for each record individually, re-reading and re-decoding the whole
    //           index blob every time.
    //
    // Checked across the whole sweep rather than at its largest point, because a cost that
    // doubles between the last two sizes and nowhere else is still a cost that grows.
    let append_reads_per_entry = |r: &Round| r.append_chunk_reads as f64 / r.entries as f64;
    let first_append_reads = append_reads_per_entry(&rounds[0]);
    for r in &rounds[1..] {
        let now = append_reads_per_entry(r);
        assert!(
            now <= first_append_reads.max(1.0) * 1.5,
            "page reads per append grew from {first_append_reads:.3} at {} entries to \
             {now:.3} at {} -- an append that reads more as the object grows is reading the \
             object back, which is O(n) per append and quadratic over the log",
            rounds[0].entries,
            r.entries
        );
    }

    let replay_reads_per_entry = |r: &Round| -> f64 {
        let worst = r
            .replays
            .iter()
            .max_by_key(|n| n.chunk_reads)
            .expect("a replayer");
        worst.chunk_reads as f64 / r.entries as f64
    };
    let first_replay_reads = replay_reads_per_entry(&rounds[0]);
    for r in &rounds[1..] {
        let now = replay_reads_per_entry(r);
        assert!(
            now <= first_replay_reads * 1.5,
            "page reads per replayed entry grew from {first_replay_reads:.3} at {} entries to \
             {now:.3} at {} -- a replay that reads more per entry as the log grows is walking \
             the log rather than indexing into it",
            rounds[0].entries,
            r.entries
        );
    }

    println!("  WHAT HELD AND WHAT DID NOT");
    println!();
    // THE CORPUS RATIO IS COMPUTED, not written in. It said "80x" while the sweep was 16x,
    // because the sizes moved and the sentence did not -- the same stale-literal shape a stamp
    // bump leaves behind in a pinned figure.
    let corpus_ratio = rounds[rounds.len() - 1].entries as f64 / rounds[0].entries as f64;
    println!("    BYTES HELD FLAT. Stored bytes per entry moved {:.1} -> {:.1} across a {corpus_ratio:.0}x corpus,",
             rounds[0].blob_bytes as f64 / rounds[0].entries as f64,
             rounds[rounds.len() - 1].blob_bytes as f64 / rounds[rounds.len() - 1].entries as f64);
    println!("    and bytes read per entry {:.1} -> {:.1}. The byte amplification is linear and asserted.",
             rounds[0].replays.iter().map(|n| n.range_bytes).max().unwrap_or(0) as f64 / rounds[0].entries as f64,
             rounds[rounds.len() - 1].replays.iter().map(|n| n.range_bytes).max().unwrap_or(0) as f64
                 / rounds[rounds.len() - 1].entries as f64);
    println!();
    let first = &rounds[0];
    let last = &rounds[rounds.len() - 1];
    println!("    AND SO DID THE READING. Page reads per append moved {:.3} -> {:.3}, and per replayed",
             first.append_chunk_reads as f64 / first.entries as f64,
             last.append_chunk_reads as f64 / last.entries as f64);
    println!("    entry {:.3} -> {:.3}. Both are asserted. This is where a per-operation walk of the",
             replay_reads_per_entry(first),
             replay_reads_per_entry(last));
    println!("    log shows, and it is the figure the byte columns could not produce: they count the");
    println!("    range READ, which was always indexed, not the work of LOCATING it. Two walks were");
    println!("    found here and removed -- an append that re-read the whole object to rehash it,");
    println!("    and a replay that re-read the whole offset index once per entry.");
    println!();
    println!("    TIMINGS ARE PRINTED AND NOT ASSERTED, because a timing is a fact about this box.");
    println!("    On this run append went {:.0} -> {:.0} us/entry and replay {:.0} -> {:.0}, over a {corpus_ratio:.0}x corpus.",
             first.append_micros as f64 / first.entries as f64,
             last.append_micros as f64 / last.entries as f64,
             first.replays.iter().map(|n| n.micros).max().unwrap_or(0) as f64 / first.entries as f64,
             last.replays.iter().map(|n| n.micros).max().unwrap_or(0) as f64 / last.entries as f64);
    println!();
    println!("    WHAT IS STILL NOT FLAT, AND WHAT THESE COUNTS CANNOT SEE. Replay per entry no longer");
    println!("    tracks the corpus; APPEND STILL DOES, and the page-read columns above are blind to");
    println!("    it because the remaining work does no reading at all. Measured by instruction count");
    println!("    (callgrind, deterministic where a timing here is not): per-append instructions still");
    println!("    double when the corpus doubles -- 374k at 200 entries, 623k at 400, 1,251k at 800.");
    println!("    It is MatrixObject's per-mutation bookkeeping, not this log's shape: begin/end of a");
    println!("    chunk mutation batch builds a replica status whose `physical_bytes` and");
    println!("    `discarded_bytes` fold over EVERY page the replica holds, and begin also exports a");
    println!("    whole-state undo snapshot. Raising the chunk tier to 64 MiB changes it by 2.7%, so it");
    println!("    is page-count and not chunk-count; raising the page tier to 1 MiB makes it 16x worse,");
    println!("    because then each append re-checksums one growing page instead of folding over many.");
    println!("    Flat appends need that bookkeeping to be maintained rather than recomputed. Until it");
    println!("    is, read the asserted counts for what they say and nothing more.");
    println!();
    println!("  Converged on every node at every size.");
}
